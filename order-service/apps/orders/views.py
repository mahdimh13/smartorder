from rest_framework import serializers, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from apps.orders.models import Order, OrderItem
from apps.orders.services import OrderService, InsufficientStockError


# --- Serializers ---

class OrderItemSerializer(serializers.ModelSerializer):
    product_name = serializers.CharField(source="product.name", read_only=True)
    subtotal = serializers.DecimalField(max_digits=10, decimal_places=2, read_only=True)

    class Meta:
        model = OrderItem
        fields = ["id", "product_id", "product_name", "quantity", "unit_price", "subtotal"]


class OrderSerializer(serializers.ModelSerializer):
    items = OrderItemSerializer(many=True, read_only=True)

    class Meta:
        model = Order
        fields = ["id", "status", "total_amount", "items", "created_at", "updated_at"]


class CreateOrderItemInput(serializers.Serializer):
    product_id = serializers.UUIDField()
    quantity = serializers.IntegerField(min_value=1)


class CreateOrderInput(serializers.Serializer):
    items = CreateOrderItemInput(many=True)


# --- Views ---

@api_view(["POST"])
@permission_classes([IsAuthenticated])
def create_order(request):
    serializer = CreateOrderInput(data=request.data)
    serializer.is_valid(raise_exception=True)

    try:
        order = OrderService.create_order(
            user=request.user,
            items=serializer.validated_data["items"],
        )
    except InsufficientStockError as e:
        return Response({"error": str(e)}, status=status.HTTP_409_CONFLICT)

    return Response(OrderSerializer(order).data, status=status.HTTP_201_CREATED)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def list_orders(request):
    orders = (
        Order.objects.filter(user=request.user)
        .prefetch_related("items__product")
        .order_by("-created_at")
    )
    return Response(OrderSerializer(orders, many=True).data)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_order(request, order_id):
    try:
        order = (
            Order.objects.prefetch_related("items__product")
            .get(id=order_id, user=request.user)
        )
    except Order.DoesNotExist:
        return Response({"error": "Order not found."}, status=status.HTTP_404_NOT_FOUND)

    return Response(OrderSerializer(order).data)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def cancel_order(request, order_id):
    try:
        order = Order.objects.get(id=order_id, user=request.user)
    except Order.DoesNotExist:
        return Response({"error": "Order not found."}, status=status.HTTP_404_NOT_FOUND)

    from django.core.exceptions import ValidationError
    try:
        order = OrderService.cancel_order(order, cancelled_by="user")
    except ValidationError as e:
        return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

    return Response(OrderSerializer(order).data)
