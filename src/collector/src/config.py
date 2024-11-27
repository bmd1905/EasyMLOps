import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    V_API: str = os.getenv("V_API", "v1")
    COLLECTOR_PATH: str = os.getenv("COLLECTOR_PATH", "/collector")
    TESTING: bool = os.getenv("TESTING", False)
    DEBUG: bool = os.getenv("DEBUG", False)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
