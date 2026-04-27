from fastapi import  BackgroundTasks, Depends, FastAPI, HTTPException, Request
from db.config import get_settings
from db.database import engine,Base,get_db
from db.schemes import  ProductCreate,ProductResponse
from db .models import Product
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from contextlib import asynccontextmanager
from uuid import UUID
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger=logging.getLogger(__name__)

settings=get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"{settings.service_name} starting...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    logger.info(f"{settings.service_name} started")

    yield
    logger.info(f"{settings.service_name} shutting down...")
    await engine.dispose()
    logger.info(f"{settings.service_name} stopped")


app=FastAPI(
    title="Product Service",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/health/live")
async def liveness():
    return {"status":"alive"}


@app.get("/health/ready")
async def readiness(db:AsyncSession=Depends(get_db)):
    try:
        await db.execute(select(1))
        return {"status":"ready"}
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not ready")
    
    
@app.post("/product",response_model=ProductResponse,status_code=201)
async def create_product(
    product:ProductCreate,
    request:Request,
    db:AsyncSession=Depends(get_db)
):
    
    db_product=Product(
        customer_id=product.customer_id,
        product_name=product.product_name,
        price=product.price
    )

    db.add(db_product)
    await db.flush()

    await db.commit()
    await db.refresh(db_product)
    logger.info(f"product created: {db_product.id} for customer {product.customer_id}")
    return db_product

