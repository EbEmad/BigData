from fastapi import Depends,FastAPI,HTTPException,Request
from sqlalchemy.ext.asyncio import AsyncSession
from db.config import get_settings
from db.database import get_db,engine,Base
from contextlib import asynccontextmanager
from sqlalchemy import select
from uuid import UUID
from uuid_extensions import uuid7
import asyncio
import logging

from db.schemes import PersonCreate,PersonResponse
from db.models import Person
settings=get_settings()
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app:FastAPI):
    logger.info(f"{settings.service_name} starting...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield

    logger.info(f"{settings.service_name} shutting down...")
    await engine.dispose()



app=FastAPI(title="customer Service",version="1.0.0",lifespan=lifespan)


@app.get("/health/live")
async def liveness():
    return {"status": "alive"}

@app.get("/health/ready")
async def readiness(db:AsyncSession=Depends(get_db)):
    try:
        await db.execute(select(1))
        return {"status": "ready"}
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service is not ready")
@app.post("/person",response_model=PersonResponse,status_code=201)
async def create_person(person:PersonCreate,db:AsyncSession=Depends(get_db)):
    person_id=uuid7()
    obj=Person(
        id=person_id,
        name=person.name,
        age=person.age,
        salary=person.salary
    )
    db.add(obj)
    await db.commit()
    await  db.refresh(obj)
    logger.info(f"person created: {person_id}")
    return obj
@app.get("/person",response_model=list[PersonResponse])
async def list_persons(skip:int=0,limit: int = 100,db:AsyncSession=Depends(get_db)):
    result=await db.execute(
        select(Person).offset(skip).limit(limit)
    )

    return result.scalars().all()

@app.get("/person/{person_id}",response_model=PersonResponse)
async def get_person(person_id:UUID,request:Request,db:AsyncSession=Depends(get_db)):
    client_ip=request.client.host if request.client else "unknown"
    result=await db.execute(select(Person).where(Person.id==person_id))
    person=result.scalar_one_or_none()

    if not person:
        raise  HTTPException(status_code=404, detail="Person not found")

    return person






