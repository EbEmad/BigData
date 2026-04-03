from .database import Base
from sqlalchemy import Column,Integer,String

class Persons(Base):
    __tablename__="persons"
    
    id= Column(Integer,primary_key=True)
    name=Column(String(50))
    age=Column(Integer)


