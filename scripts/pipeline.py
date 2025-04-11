import logging

from prefect import flow, task

from scripts.data_extraction.extract import ZoneExtractor, load_zone_configs
from scripts.data_processing.transform import ElectricityMapsDataTransformer

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

logger = logging.getLogger(__name__)


@task
def extract():
    """
    Extracts data from the electricity maps API for all zones defined in the config file.
    """
    zones_config = load_zone_configs()
    for zone, token in zones_config.items():
        ZoneExtractor(zone=zone, token=token).extract()


@task
def transform():
    """
    Transforms the raw data extracted from the electricity maps API into a structured format
    and loads it into a DuckDB database.
    """
    transformer = ElectricityMapsDataTransformer(
        raw_data_folder="data/raw", db_path="data/processed/db.duckdb"
    )
    transformer.process_all_files()


@flow(
    name="Electricity Carbon Intensity Pipeline",
)
def electricity_carbon():
    """
    Main flow to extract and transform electricity carbon intensity data.
    """
    extract()
    transform()
    logger.info(f"Pipeline completed! Refresh your Streamlit app.")


if __name__ == "__main__":
    electricity_carbon()
