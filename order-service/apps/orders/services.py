import uuid
import logging
from decimal import Decimal
from django.db import transaction
from django.core.exceptions import ValidationError

from apps.orders.models import Order, OrderItem
from apps.products.models import Product
from apps.outbox.publisher import publish_event

logger = logging.getLogger(__name__)


class InsufficientStockError(Exception):
    pass


class OrderService:

    @staticmethod
    def create_order(user, items: list[dict]) -> Order:
        """
        Create an order and publish order.created event atomically.

        items: [{"product_id": uuid, "quantity": int}, ...]

        Both the DB write and the outbox event happen in one transaction.
        If anything fails, both roll back — no orphan events, no orphan orders.
        """
        if not items:
            raise ValidationError("Order must have at least one item.")

        with transaction.atomic():
            # Lock products for update to prevent race conditions on stock
            product_ids = [item["product_id"] for item in items]
            products = {
                str(p.id): p
                for p in Product.objects.select_for_update().filter(id__in=product_ids)
            }

            for item in items:
                product = products.get(str(item["product_id"]))
                if not product:
                    raise ValidationError(f"Product {item['product_id']} not found.")
                if product.stock < item["quantity"]:
                    raise InsufficientStockError(
                        f"Insufficient stock for '{product.name}'. "
                        f"Available: {product.stock}, requested: {item['quantity']}"
                    )

            total = Decimal("0")
            for item in items:
                product = products[str(item["product_id"])]
                total += product.price * item["quantity"]

            order = Order.objects.create(
                user=user,
                total_amount=total,
                status=Order.Status.PENDING,
            )

            for item in items:
                product = products[str(item["product_id"])]
                OrderItem.objects.create(
                    order=order,
                    product=product,
                    quantity=item["quantity"],
                    unit_price=product.price,  
                )
                product.stock -= item["quantity"]
                product.save(update_fields=["stock"])

            # Publish to outbox (same transaction)
            publish_event(
                topic="order.created",
                payload={
                    "event_id": str(uuid.uuid4()),
                    "event_type": "order.created",
                    "order_id": str(order.id),
                    "user_id": str(user.id),
                    "amount": str(order.total_amount),
                    "idempotency_key": str(order.idempotency_key),
                    "items": [
                        {
                            "product_id": str(item["product_id"]),
                            "quantity": item["quantity"],
                        }
                        for item in items
                    ],
                },
            )

            logger.info("Order %s created for user %s", order.id, user.id)
            return order

    @staticmethod
    def cancel_order(order: Order, cancelled_by: str = "user") -> Order:
        """Cancel an order and notify payment service via Kafka."""
        if order.status not in (Order.Status.PENDING, Order.Status.CONFIRMED):
            raise ValidationError(
                f"Cannot cancel order in status '{order.status}'."
            )

        with transaction.atomic():
            # Restore stock
            for item in order.items.select_related("product").select_for_update():
                item.product.stock += item.quantity
                item.product.save(update_fields=["stock"])

            order.status = Order.Status.CANCELLED
            order.save(update_fields=["status", "updated_at"])

            publish_event(
                topic="order.cancelled",
                payload={
                    "event_id": str(uuid.uuid4()),
                    "event_type": "order.cancelled",
                    "order_id": str(order.id),
                    "cancelled_by": cancelled_by,
                },
            )

        return order

    @staticmethod
    def mark_paid(order_id: str, payment_id: str) -> Order:
        """Called by Kafka consumer when payment.confirmed is received."""
        with transaction.atomic():
            order = Order.objects.select_for_update().get(id=order_id)
            if order.status != Order.Status.PENDING:
                logger.warning(
                    "Received payment.confirmed for order %s in status %s — skipping",
                    order_id, order.status
                )
                return order
            order.status = Order.Status.PAID
            order.save(update_fields=["status", "updated_at"])
            logger.info("Order %s marked as PAID (payment %s)", order_id, payment_id)
        return order

    @staticmethod
    def mark_failed(order_id: str, reason: str) -> Order:
        """Called by Kafka consumer when payment.failed is received."""
        with transaction.atomic():
            order = Order.objects.select_for_update().get(id=order_id)
            if order.status != Order.Status.PENDING:
                return order

            # Restore stock on payment failure
            for item in order.items.select_related("product").select_for_update():
                item.product.stock += item.quantity
                item.product.save(update_fields=["stock"])

            order.status = Order.Status.FAILED
            order.save(update_fields=["status", "updated_at"])
            logger.warning("Order %s marked as FAILED: %s", order_id, reason)
        return order
