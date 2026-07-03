.PHONY: up down logs test migrate seed

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f

migrate:
	docker compose exec order-service python manage.py migrate
	docker compose exec payment-service python -c "import asyncio; from app.db.session import init_db; asyncio.run(init_db())"

seed:
	docker compose exec order-service python manage.py seed_data

test-order:
	docker compose exec order-service python -m pytest tests/ -v

test-payment:
	docker compose exec payment-service python -m pytest tests/ -v

test:
	make test-order
	make test-payment

stress:
	docker compose exec order-service python scripts/stress_test.py
