from db.database import get_db,engine,Base
from db.config import get_settings
from db.schemes import Person
from db.model import Persons
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fastapi import Depends,FastAPI,Request, HTTPException
import logging
from contextlib import asynccontextmanager
from utils.metrics import setup_metrics

settings=get_settings()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app:FastAPI):
    logger.info("Starting up the application")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield

    logger.info("Shutting down the application")

    engine.dispose()

app=FastAPI(
    title="Document Service",version="1.0.0",lifespan=lifespan
)
setup_metrics(app)

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
        raise HTTPException(status_code=503, detail="Service is not ready")

@app.post("/insert")
async def insert_person(person:Person,db:AsyncSession=Depends(get_db)):
    """Create person with async pg upload."""
    object=Persons(
        id=person.id,
        name=person.name,
        age=person.age
    )

    db.add(object)
    await db.commit()
    await db.refresh(object)
    logger.info(f"Person created: {object}")
    return object