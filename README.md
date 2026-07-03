# SmartOrder

An event-driven order & payment platform built as two independent microservices that communicate asynchronously over Kafka. Designed to demonstrate reliable, at-least-once event delivery between services using the transactional outbox pattern, idempotent payment processing, and optimistic-safe stock management.

## Architecture

```
┌─────────────────────┐        order.created        ┌──────────────────────┐
│    Order Service     │ ───────────────────────────▶│   Payment Service     │
│   (Django + DRF +    │        order.cancelled       │  (FastAPI + async)   │
│    Strawberry GraphQL)│                              │                      │
│                       │◀───────────────────────────│                      │
│                       │  payment.confirmed / failed  │                      │
└──────────┬────────────┘                              └──────────┬───────────┘
           │                                                       │
     ┌─────▼─────┐                                            ┌────▼─────┐
     │ order-db  │                                            │payment-db│
     │(Postgres) │                                            │(Postgres)│
     └───────────┘                                            └──────────┘
                              ┌─────────────┐
                              │    Kafka    │
                              └─────────────┘
                              ┌─────────────┐
                              │    Redis    │
                              └─────────────┘
```

Each service owns its own Postgres database — there is no shared schema or foreign key between them. All cross-service communication happens through Kafka events, keeping the services independently deployable and failure-isolated.

### Order Service (Django)

- REST API (Django REST Framework) for creating, listing, retrieving, and cancelling orders
- GraphQL API (Strawberry) for reporting: a customer's own orders, monthly revenue, top-selling products, and order counts by status
- Stock is decremented with `select_for_update()` inside a DB transaction to prevent overselling under concurrent requests
- Writes outgoing events (`order.created`, `order.cancelled`) to an **outbox table** in the same transaction as the order write, then a separate background worker polls that table and publishes to Kafka — so an order is never created without its event eventually being sent, and vice versa
- A Kafka consumer listens for `payment.confirmed` / `payment.failed` and updates order status (restoring stock automatically on failure)

### Payment Service (FastAPI)

- Async FastAPI app with its own Postgres database (via SQLAlchemy + `asyncpg`)
- Kafka consumer processes `order.created` events and simulates a charge against a payment provider
- Idempotency is enforced with a Redis cache (fast path) backed by a unique DB constraint on `idempotency_key` (fallback), so a redelivered Kafka message never results in a duplicate charge
- Publishes `payment.confirmed` or `payment.failed` back to Kafka
- Exposes a webhook endpoint for provider callbacks (e.g. Stripe)

### Why the outbox pattern?

Writing to the database and publishing to Kafka are two separate operations that can't be wrapped in a single distributed transaction. Instead, the order write and the event both land in the same Postgres transaction (the event just goes into an `outbox` table instead of Kafka directly). A dedicated worker then drains that table into Kafka with `acks=all`, retrying anything that fails. This guarantees at-least-once delivery without ever losing an event or creating an order with no corresponding event.

## Tech Stack

| | Order Service | Payment Service |
|---|---|---|
| Framework | Django + Django REST Framework | FastAPI |
| API | REST + GraphQL (Strawberry) | REST |
| Database | PostgreSQL | PostgreSQL |
| ORM | Django ORM | SQLAlchemy (async) |
| Messaging | kafka-python | aiokafka |
| Cache | Redis | Redis (idempotency store) |

**Shared infrastructure:** Kafka + Zookeeper (event bus), Redis, PostgreSQL (one instance per service).

## Getting Started

### Prerequisites

- Docker and Docker Compose

### Run the stack

```bash
make up          # build and start all services
make migrate      # run DB migrations for both services
make seed         # seed sample product data
```

The services will be available at:

- Order Service (REST + GraphQL): `http://localhost:8000`
- Payment Service: `http://localhost:8001`

Other useful commands:

```bash
make logs         # tail logs from all containers
make test         # run the test suite for both services
make stress       # run the stress test script against the Order Service
make down         # stop and remove all containers/volumes
```

## Event Flow

1. Client calls `POST /api/orders` on the Order Service.
2. Order Service validates stock, creates the order (`PENDING`), decrements stock, and writes an `order.created` event to its outbox — all in one transaction.
3. The outbox worker publishes `order.created` to Kafka.
4. Payment Service consumes `order.created`, checks idempotency, and processes the charge.
5. Payment Service publishes `payment.confirmed` or `payment.failed`.
6. Order Service consumes that event and updates the order to `PAID` or `FAILED` (restoring stock on failure).

## Project Structure

```
order-service/
├── apps/
│   ├── orders/       # Order & OrderItem models, REST views, business logic, Kafka consumer
│   ├── products/     # Product & Category models
│   ├── users/         # Custom user model with roles (MERCHANT / CUSTOMER)
│   └── outbox/        # Outbox pattern: models, publisher, background worker
├── graphql/
│   └── schema.py      # Strawberry GraphQL schema (reporting queries)
└── config/             # Django settings

payment-service/
├── app/
│   ├── api/            # REST routes, request/response schemas, webhook handler
│   ├── core/            # Payment processing logic (idempotency, mock provider charge)
│   ├── db/               # SQLAlchemy models & async session
│   └── kafka/           # Kafka consumer/producer
```

## Notes

- Payment provider integration is mocked (`_charge_payment`) — swap in a real payment SDK call to go to production.
- Secrets in `docker-compose.yml` (JWT secret, webhook secret) are placeholders for local development only and must be replaced before deploying anywhere real.
