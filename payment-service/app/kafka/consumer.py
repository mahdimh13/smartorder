import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from decimal import Decimal
from app.config import settings
from app.db.session import get_session
from app.core.payment import process_payment

logger = logging.getLogger(__name__)


# --- Producer ---

_producer: AIOKafkaProducer | None = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await _producer.start()
    return _producer


async def publish_event(topic: str, payload: dict) -> None:
    producer = await get_producer()
    await producer.send_and_wait(topic, value=payload)
    logger.info("Published %s to %s", payload.get("event_type"), topic)


# --- Consumer ---

async def run_consumer() -> None:
    """
    Async Kafka consumer for the Payment Service.
    Listens for order.created and order.cancelled events.
    """
    consumer = AIOKafkaConsumer(
        "order.created",
        "order.cancelled",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="payment-service-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    await consumer.start()
    logger.info("Payment Service consumer started.")

    try:
        async for message in consumer:
            event = message.value
            event_type = event.get("event_type")
            event_id = event.get("event_id")

            logger.info("Received event: %s (id=%s)", event_type, event_id)

            try:
                if event_type == "order.created":
                    async with get_session() as db:
                        await process_payment(
                            db=db,
                            order_id=event["order_id"],
                            amount=Decimal(event["amount"]),
                            idempotency_key=event["idempotency_key"],
                        )

                elif event_type == "order.cancelled":
                    logger.info(
                        "Order %s cancelled — aborting any pending payment",
                        event["order_id"],
                    )
                    # In a real system: cancel in-flight payment PaymentIntent here

                await consumer.commit()

            except Exception as exc:
                logger.error(
                    "Failed to process event %s (id=%s): %s",
                    event_type, event_id, exc
                )
                # No commit — will retry on restart

    finally:
        await consumer.stop()
