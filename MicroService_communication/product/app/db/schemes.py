from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, validator


class ProductCreate(BaseModel):
    customer_id:UUID
    product_name:str=Field(..., min_length=1, max_length=255)
    price:int=Field(..., ge=200, le=1000)



class ProductResponse(BaseModel):
    id:UUID
    customer_id:UUID
    product_name:str
    price:int

    class Config:
        from_attributes = True
