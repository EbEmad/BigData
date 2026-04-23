from enum import Enum
from uuid_extensions import uuid7
from datetime import datetime
from typing import ClassVar
from sqlalchemy import BigInteger, Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from .database import Base

class Person(Base):
    __tablename__ = "Person"
    __table_args__: ClassVar[dict] = {"schema": "Persons"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid7)
    name=Column(String(255),nullable=False)
    age=Column(Integer,nullable=False)
    salary=Column(BigInteger,nullable=False)
              