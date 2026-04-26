from uuid_extensions import uuid7
from typing import ClassVar
from datetime import datetime
from sqlalchemy import Column, DateTime, String, Text,Integer
from sqlalchemy.dialects.postgresql import UUID
from .database import Base

class Product(Base):
    __tablename__ = "products"
    __table_args__: ClassVar[dict] = {"schema": "public"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid7)
    customer_id=Column(UUID(as_uuid=True), nullable=False)
    product_name=Column(String(255), nullable=False)
    price=Column(Integer,nullable=False)