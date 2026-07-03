import uuid
from django.db import models


class OutboxEvent(models.Model):
    """
    Outbox Pattern: events are written here inside the same DB transaction
    as the business operation. A background worker then reads unpublished
    events and sends them to Kafka. This guarantees at-least-once delivery
    even if Kafka is temporarily unavailable.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    topic = models.CharField(max_length=100)  # e.g. "order.created"
    payload = models.JSONField()
    published = models.BooleanField(default=False, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    published_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["created_at"]
        indexes = [
            # Compound index — worker queries: published=False ORDER BY created_at
            models.Index(fields=["published", "created_at"], name="outbox_unpublished_idx"),
        ]

    def __str__(self):
        return f"{self.topic} | published={self.published}"
