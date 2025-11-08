import json
import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

# File paths for config files
_app_dir_path = os.path.dirname(os.path.abspath(__file__))
_config_dir_path = os.path.join(_app_dir_path, "..", "config")
_EIC_MAPPINGS_FILE_PATH = os.path.join(_config_dir_path, "eic_mappings.json")
_PSR_TYPE_MAPPINGS_FILE_PATH = os.path.join(_config_dir_path, "psr_type_mappings.json")


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
    EIC_MAPPINGS: dict = _load_eic_codes_from_json(_EIC_MAPPINGS_FILE_PATH)
    PSR_TYPE_MAPPINGS: dict = _load_psr_types_from_json(_PSR_TYPE_MAPPINGS_FILE_PATH)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

settings = Settings()