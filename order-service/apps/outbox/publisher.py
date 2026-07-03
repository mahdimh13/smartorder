from django.utils import timezone
from apps.outbox.models import OutboxEvent


def publish_event(topic: str, payload: dict) -> OutboxEvent:
    """
    Write an event to the outbox table.

    MUST be called inside the same DB transaction as the business operation.
    Example:

        with transaction.atomic():
            order = Order.objects.create(...)
            publish_event("order.created", {"order_id": str(order.id), ...})

    The outbox worker will pick this up and send it to Kafka.
    Never call this outside a transaction.atomic() block.
    """
    return OutboxEvent.objects.create(topic=topic, payload=payload)
