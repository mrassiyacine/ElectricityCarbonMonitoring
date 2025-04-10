# scripts/data_extraction/extract.py
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import yaml
from dotenv import load_dotenv

from scripts.data_extraction.electricity_maps_client import ElectricityMapsClient

load_dotenv()

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

logger = logging.getLogger(__name__)


def load_zone_configs() -> dict:
    """
    Load zone configurations from YAML file.
    returns:
        dict: The zone configurations.
    """
    with open("config/api_config.yaml") as f:
        config = yaml.safe_load(f)
    zones = {}
    for zone, cfg in config["zones"].items():
        if cfg["active"]:
            cfg["token"] = os.getenv(cfg["token_env"])
            if not cfg["token"]:
                raise ValueError(f"Missing env var: {cfg['token_env']}")
            zones[zone] = cfg["token"]
    logger.info(f"Loaded {len(zones)} active zones")
    return zones


class ZoneExtractor:
    """
    Extracts data from the ElectricityMap API for a given zone.
    """

    def __init__(self, zone: str, token: str):
        self.zone = zone
        self.client = ElectricityMapsClient(zone=zone, token=token)

    def extract(self) -> List[Path]:
        """
        Returns list of saved file paths
        returns:
            List[Path]: List of saved file paths.
        """
        data_types = [
            ("carbon_intensity", self.client.get_carbon_intensity),
            ("power_breakdown", self.client.get_power_breakdown),
        ]
        saved_files = []
        for data_type, fetcher in data_types:
            try:
                data = fetcher()
                path = self._save(data, data_type)
                saved_files.append(path)
                logger.info(f"{self.zone} {data_type} data saved")
            except Exception as e:
                logger.error(f"{self.zone} {data_type} failed: {e}")
        return saved_files

    def _save(self, data: Dict, data_type: str) -> Path:
        """
        Zone-aware file saving
        args:
            data (Dict): The data to save.
            data_type (str): The type of data.
        returns:
            Path: The path to the saved file.
        """
        date = datetime.now(timezone.utc).strftime("%Y_%m_%d")
        filename = f"{self.zone}_{data_type}_{date}.json"
        path = Path(f"data/raw/{filename}")
        path.parent.mkdir(exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f)
        return path


if __name__ == "__main__":
    zones = load_zone_configs()
    for zone, token in zones.items():
        extractor = ZoneExtractor(zone, token)
        extractor.extract()
