import strawberry
from strawberry.types import Info
from strawberry.permission import BasePermission
from typing import List, Optional
from decimal import Decimal
from datetime import datetime
from django.db.models import Sum, Count
from django.db.models.functions import TruncMonth

from apps.orders.models import Order, OrderItem
from apps.products.models import Product


# --- Permissions ---

class IsAuthenticated(BasePermission):
    message = "Authentication required."

    def has_permission(self, source, info: Info, **kwargs) -> bool:
        request = info.context["request"]
        return request.user and request.user.is_authenticated


# --- Types ---

@strawberry.type
class OrderItemType:
    id: strawberry.ID
    product_id: strawberry.ID
    product_name: str
    quantity: int
    unit_price: Decimal
    subtotal: Decimal


@strawberry.type
class OrderType:
    id: strawberry.ID
    status: str
    total_amount: Decimal
    created_at: datetime
    items: List[OrderItemType]


@strawberry.type
class RevenueByMonth:
    month: str
    total_revenue: Decimal
    order_count: int


@strawberry.type
class TopProduct:
    product_id: strawberry.ID
    name: str
    units_sold: int
    revenue: Decimal


@strawberry.type
class OrdersByStatus:
    status: str
    count: int


# --- Resolvers ---

def resolve_order_items(order: Order) -> List[OrderItemType]:
    return [
        OrderItemType(
            id=str(item.id),
            product_id=str(item.product_id),
            product_name=item.product.name,
            quantity=item.quantity,
            unit_price=item.unit_price,
            subtotal=item.subtotal,
        )
        for item in order.items.select_related("product").all()
    ]


# --- Query ---

@strawberry.type
class Query:

    @strawberry.field(permission_classes=[IsAuthenticated])
    def my_orders(self, info: Info) -> List[OrderType]:
        user = info.context["request"].user
        orders = (
            Order.objects.filter(user=user)
            .prefetch_related("items__product")
            .order_by("-created_at")
        )
        return [
            OrderType(
                id=str(o.id),
                status=o.status,
                total_amount=o.total_amount,
                created_at=o.created_at,
                items=resolve_order_items(o),
            )
            for o in orders
        ]

    @strawberry.field(permission_classes=[IsAuthenticated])
    def revenue_by_month(self, info: Info, year: int) -> List[RevenueByMonth]:
        qs = (
            Order.objects.filter(
                status=Order.Status.PAID,
                created_at__year=year,
            )
            .annotate(month=TruncMonth("created_at"))
            .values("month")
            .annotate(
                total_revenue=Sum("total_amount"),
                order_count=Count("id"),
            )
            .order_by("month")
        )
        return [
            RevenueByMonth(
                month=row["month"].strftime("%Y-%m"),
                total_revenue=row["total_revenue"] or Decimal("0"),
                order_count=row["order_count"],
            )
            for row in qs
        ]

    @strawberry.field(permission_classes=[IsAuthenticated])
    def top_products(self, info: Info, limit: int = 5) -> List[TopProduct]:
        qs = (
            OrderItem.objects.filter(order__status=Order.Status.PAID)
            .values("product_id", "product__name")
            .annotate(
                units_sold=Sum("quantity"),
                revenue=Sum("unit_price"),
            )
            .order_by("-units_sold")[:limit]
        )
        return [
            TopProduct(
                product_id=str(row["product_id"]),
                name=row["product__name"],
                units_sold=row["units_sold"],
                revenue=row["revenue"] or Decimal("0"),
            )
            for row in qs
        ]

    @strawberry.field(permission_classes=[IsAuthenticated])
    def orders_by_status(self, info: Info) -> List[OrdersByStatus]:
        qs = (
            Order.objects.values("status")
            .annotate(count=Count("id"))
            .order_by("status")
        )
        return [
            OrdersByStatus(status=row["status"], count=row["count"])
            for row in qs
        ]


schema = strawberry.Schema(query=Query)
