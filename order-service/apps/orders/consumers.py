import json
import logging
from kafka import KafkaConsumer
from django.conf import settings
from apps.orders.services import OrderService

logger = logging.getLogger(__name__)

TOPICS = ["payment.confirmed", "payment.failed"]


def run_consumer():
    """
    Long-running Kafka consumer for the Order Service.
    Runs as a Django management command.

    Handles:
      - payment.confirmed → mark order PAID
      - payment.failed    → mark order FAILED, restore stock
    """
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="order-service-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # manual commit after processing
    )

    logger.info("Order Service consumer started. Listening on: %s", TOPICS)

    for message in consumer:
        event = message.value
        event_type = event.get("event_type")
        event_id = event.get("event_id")

        logger.info("Received event: %s (id=%s)", event_type, event_id)

        try:
            if event_type == "payment.confirmed":
                OrderService.mark_paid(
                    order_id=event["order_id"],
                    payment_id=event["payment_id"],
                )

            elif event_type == "payment.failed":
                OrderService.mark_failed(
                    order_id=event["order_id"],
                    reason=event.get("reason", "unknown"),
                )

            else:
                logger.warning("Unknown event type: %s — skipping", event_type)

            # Only commit offset after successful processing
            consumer.commit()

        except Exception as exc:
            logger.error(
                "Failed to process event %s (id=%s): %s",
                event_type, event_id, exc
            )
            # Do not commit — message will be re-delivered on restart
