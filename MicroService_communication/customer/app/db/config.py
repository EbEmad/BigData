from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    service_name: str = "document-service"
    database_url:str

@lru_cache
def get_settings():
    return Settings()