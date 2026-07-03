from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@payment-db:5432/payment_db"
    REDIS_URL: str = "redis://redis:6379/0"
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    JWT_SECRET: str = "change-me-in-production"
    PAYMENT_WEBHOOK_SECRET: str = ""

    class Config:
        env_file = ".env"


settings = Settings()
