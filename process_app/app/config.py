from dotenv import load_dotenv
from pydantic_settings import BaseSettings
import json

load_dotenv()

def _load_eic_codes_from_json(filepath: str) -> dict:
    try:        
        with open(filepath, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def _load_psr_types_from_json(filepath: str) -> dict:
    try:
        with open(filepath, "r") as f:
            psr_types_json = json.load(f)
        
        psr_types = {}
        for obj in psr_types_json:
            psr_types[obj["code"]] = {
                "name": obj["name"],
                "kg_co2e_mwh": obj["kg_co2e_mwh"]
            }
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
    PSR_TYPE_MAPPINGS: dict = _load_psr_types_from_json("config/psr_type_mappings.json")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()