from datetime import datetime
from uuid import UUID
from pydantic import Field, field_validator,BaseModel

class PersonCreate(BaseModel):
    name:str=Field(..., min_length=1, max_length=255)
    age:int
    salary:int

    @field_validator("age")
    def validate_age(cls,v:str)->str:
        if v<15:
            raise ValueError("invalid case")
        return v


class PersonResponse(BaseModel):
    id:UUID
    name:str
    age:int
    salary:int

    class Config:
        from_attributes = True