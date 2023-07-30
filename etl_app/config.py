from typing import Literal, Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Django
    DJANGO_DEBUG: Optional[bool]
    DJANGO_SECRET_KEY: Optional[str]
    DJANGO_ALLOWED_HOSTS: Optional[str]

    # PostgreSQL
    POSTGRES_DB_NAME: str
    POSTGRES_DB_USER: str
    POSTGRES_DB_PASS: str
    POSTGRES_DB_HOST: str
    POSTGRES_DB_PORT: str
    POSTGRES_DB_OPTS: str

    # SQLite
    SQLITE_DB_NAME: Optional[str]

    # Elastic
    ELASTIC_HOST: str
    ELASTIC_PORT: str
    ELASTIC_INDEX: str
    ELASTIC_INDEX_FILE: str

    # ETL
    ETL_STATE_FILENAME: str
    ETL_TIMEOUT_SEC: int
    ETL_EXTRACT_BATCH: int
    ETL_LOAD_BATCH: int

    @property
    def es_url(self):
        return f'http://{self.ELASTIC_HOST}:{self.ELASTIC_PORT}'

    @property
    def es_url_with_index(self):
        return f'http://{self.ELASTIC_HOST}:{self.ELASTIC_PORT}/{self.ELASTIC_INDEX}'

    # Logger
    DEBUG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    class Config:
        env_file = ".env"


settings = Settings()
