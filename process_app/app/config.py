from csv import DictReader
from dotenv import load_dotenv
from typing import List
from pydantic_settings import BaseSettings
import json

load_dotenv()

def _load_eic_codes_from_json(filepath: str) -> dict:
    try:        
        with open(filepath, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def _load_psr_types_from_csv(filepath: str) -> dict:
    try:
        with open(filepath, "r") as f:
            reader = DictReader(f)
            psr_types = {row["psr_type_code"]: row["psr_type_name"] for row in reader}
            return psr_types
    except FileNotFoundError:
        return {}

class Settings(BaseSettings):
    """A class to manage all application settings."""

    # Secrets
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_SASL_USERNAME: str
    KAFKA_SASL_PASSWORD: str

    # Application constants
    RAW_GENERATION_TOPIC: str = "raw-generation-events"
    ENRICHED_GENERATION_TOPIC: str = "enriched-generation-events"
    EIC_MAPPINGS: dict = _load_eic_codes_from_json("config/eic_country_mappings.json")
    PSR_TYPE_MAPPINGS: dict = _load_psr_types_from_csv("config/psr_type_mappings.csv")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()