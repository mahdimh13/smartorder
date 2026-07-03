import json
import logging
from django.utils import timezone
from kafka import KafkaProducer
from django.conf import settings
from apps.outbox.models import OutboxEvent

logger = logging.getLogger(__name__)


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Strongest durability: wait for all in-sync replicas
        acks="all",
        retries=5,
    )


def run_outbox_worker(batch_size: int = 100) -> int:
    """
    Poll unpublished outbox events and send them to Kafka.
    Returns number of events published.

    This runs as a Django management command on a loop (every ~1s).
    In production, you'd use Celery beat or a dedicated process.
    """
    producer = get_producer()
    published_count = 0

    # select_for_update() prevents two workers from picking the same row
    events = (
        OutboxEvent.objects.select_for_update(skip_locked=True)
        .filter(published=False)
        .order_by("created_at")[:batch_size]
    )

    for event in events:
        try:
            future = producer.send(
                topic=event.topic,
                key=str(event.id),   # partition key = event id
                value=event.payload,
            )
            future.get(timeout=10)  # block until broker ack

            event.published = True
            event.published_at = timezone.now()
            event.save(update_fields=["published", "published_at"])
            published_count += 1
            logger.info("Published event %s to topic %s", event.id, event.topic)

        except Exception as exc:
            logger.error(
                "Failed to publish event %s to topic %s: %s",
                event.id, event.topic, exc
            )
            # Do NOT mark as published — worker will retry on next poll

    producer.flush()
    return published_count
