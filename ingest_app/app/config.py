from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from typing import List
import logging

load_dotenv()

def _load_eic_codes_from_file(filepath: str) -> List[str]:
    """Reads a list of EIC codes from a text file, one per line."""
    try:
        with open(filepath, 'r') as f:
            # Read all lines, strip whitespace, and filter out
            # any empty lines or lines that start with a comment (#).
            codes = [
                line.strip() for line in f
                if line.strip() and not line.strip().startswith('#')
            ]
            return codes
    except FileNotFoundError:
        logging.error(f"Couldn't find file for EIC codes: {filepath}")
        return []

class Settings(BaseSettings):
    """A class to manage all application settings."""
    # Secrets
    ENTSOE_API_KEY: str
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_SASL_USERNAME: str
    KAFKA_SASL_PASSWORD: str

    # App constants
    ENTSOE_API_URL: str = "https://web-api.tp.entsoe.eu/api"
    RAW_GENERATION_TOPIC: str = "raw-generation-events"
    EIC_CODES: List[str] = _load_eic_codes_from_file("config/eic_codes_all.txt")
    EIC_CODES_GENERATION: List[str] = _load_eic_codes_from_file("config/eic_codes_generation.txt")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra="ignore"

settings = Settings()