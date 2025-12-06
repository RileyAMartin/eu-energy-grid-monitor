import os
import logging
from typing import List
from dotenv import load_dotenv
from pydantic import ConfigDict
from pydantic_settings import BaseSettings

load_dotenv()

# File paths for config files
_app_dir_path = os.path.dirname(os.path.abspath(__file__))
_config_dir_path = os.path.join(_app_dir_path, "..", "config")
_EIC_CODES_FILE_PATH = os.path.join(_config_dir_path, "eic_codes_all.txt")
_GENERATION_EIC_CODES_FILE_PATH = os.path.join(_config_dir_path, "eic_codes_generation.txt")

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
    EIC_CODES: List[str] = _load_eic_codes_from_file(_EIC_CODES_FILE_PATH)
    EIC_CODES_GENERATION: List[str] = _load_eic_codes_from_file(_GENERATION_EIC_CODES_FILE_PATH)
    DEEP_BACKFILL_HOUR_UTC: int = 2

    model_config = ConfigDict(
        extra="ignore"
    )

settings = Settings()