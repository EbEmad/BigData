from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    database_url: str
    POSTGRES_USER:str
    POSTGRES_PASSWORD:str
    POSTGRES_DB:str
    

    
    class Config:
        env_file = ".env"

@lru_cache
def get_settings():
    return Settings()