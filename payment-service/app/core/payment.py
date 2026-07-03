import uuid
import logging
from decimal import Decimal
from datetime import datetime

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.db.models import Payment, PaymentStatus, Transaction
from app.kafka.producer import publish_event
from app.config import settings

logger = logging.getLogger(__name__)

IDEMPOTENCY_TTL = 86400  # 24 hours in seconds


async def get_redis() -> redis.Redis:
    return redis.from_url(settings.REDIS_URL, decode_responses=True)


async def process_payment(
    db: AsyncSession,
    order_id: str,
    amount: Decimal,
    idempotency_key: str,
) -> Payment:
    """
    Process a payment for an order.

    Idempotency: if the same idempotency_key comes in twice,
    return the existing payment without processing again.
    This handles Kafka at-least-once delivery.
    """
    r = await get_redis()

    # Check Redis idempotency cache first (fast path)
    cached_payment_id = await r.get(f"idempotency:{idempotency_key}")
    if cached_payment_id:
        logger.info(
            "Duplicate payment request for idempotency_key=%s — returning cached result",
            idempotency_key,
        )
        result = await db.execute(
            select(Payment).where(Payment.id == uuid.UUID(cached_payment_id))
        )
        return result.scalar_one()

    # Check DB as fallback (handles Redis eviction)
    result = await db.execute(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    existing = result.scalar_one_or_none()
    if existing:
        await r.setex(f"idempotency:{idempotency_key}", IDEMPOTENCY_TTL, str(existing.id))
        return existing

    # Create payment record
    payment = Payment(
        order_id=uuid.UUID(order_id),
        idempotency_key=idempotency_key,
        status=PaymentStatus.PENDING,
        amount=amount,
    )
    db.add(payment)
    await db.flush()  # get payment.id without committing

    try:
        # Call payment provider (mock payment)
        provider_ref = await _charge_payment(amount=amount, order_id=order_id)

        transaction = Transaction(
            payment_id=payment.id,
            provider_ref=provider_ref,
            type="CHARGE",
            raw_response={"status": "succeeded", "charge_id": provider_ref},
        )
        db.add(transaction)

        payment.status = PaymentStatus.SUCCESS
        await db.commit()

        # Cache idempotency key
        await r.setex(f"idempotency:{idempotency_key}", IDEMPOTENCY_TTL, str(payment.id))

        # Publish success event
        await publish_event(
            topic="payment.confirmed",
            payload={
                "event_id": str(uuid.uuid4()),
                "event_type": "payment.confirmed",
                "order_id": order_id,
                "payment_id": str(payment.id),
                "provider_ref": provider_ref,
            },
        )

        logger.info("Payment %s succeeded for order %s", payment.id, order_id)

    except Exception as exc:
        await db.rollback()
        payment.status = PaymentStatus.FAILED
        db.add(payment)
        await db.commit()

        await publish_event(
            topic="payment.failed",
            payload={
                "event_id": str(uuid.uuid4()),
                "event_type": "payment.failed",
                "order_id": order_id,
                "reason": str(exc),
            },
        )

        logger.error("Payment failed for order %s: %s", order_id, exc)

    return payment


async def _charge_payment(amount: Decimal, order_id: str) -> str:
    """
    Mock payment charge. Replace with real payment SDK call.
    Returns a provider reference (charge ID).
    """
    # Simulate: orders ending in "0" fail (for testing)
    if order_id.endswith("0"):
        raise Exception("card_declined")

    return f"ch_mock_{uuid.uuid4().hex[:16]}"
