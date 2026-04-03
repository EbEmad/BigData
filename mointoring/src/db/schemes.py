from pydantic import BaseModel,Field

class Person(BaseModel):
    id:int=Field(default=None,primary_key=True)
    name:str=Field(...,min_length=1,max_length=50)
    age:int=Field(...,ge=0,le=70)
    
    class Config:
        from_attributes = True
