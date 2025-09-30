import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

class Settings(BaseSettings):
    """A class to manage all application settings."""
    # Secrets
    ENTSOE_API_KEY: str
    KAFKA_BOOTSTRAP_SERVERS: str

    # Application constants
    API_URL: str = "https://web-api.tp.entsoe.eu/api"
    RAW_GENERATION_TOPIC = "raw-generation-events"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()