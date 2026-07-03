from pydantic import BaseModel, UUID4
from decimal import Decimal
from datetime import datetime
from enum import Enum
from typing import Optional


class PaymentStatus(str, Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class PaymentResponse(BaseModel):
    id: UUID4
    order_id: UUID4
    status: PaymentStatus
    amount: Decimal
    created_at: datetime

    class Config:
        from_attributes = True


class WebhookPayload(BaseModel):
    """payment webhook payload (simplified)"""
    id: str
    type: str  # e.g. "payment_intent.succeeded"
    data: dict
