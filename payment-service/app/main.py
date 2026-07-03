from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.routes import router as payment_router
from app.api.webhook import router as webhook_router
from app.db.session import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title="SmartOrder Payment Service", lifespan=lifespan)

app.include_router(payment_router, prefix="/api/v1/payments", tags=["payments"])
app.include_router(webhook_router, prefix="/webhooks", tags=["webhooks"])


@app.get("/health")
async def health():
    return {"status": "ok"}
