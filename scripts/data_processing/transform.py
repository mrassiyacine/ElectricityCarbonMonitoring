import json
import logging
import os
import shutil

import duckdb
import pandas as pd

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

logger = logging.getLogger(__name__)

power_breakdown_essential_columns = [
    "zone",
    "datetime",
    "isEstimated",
    "fossilFreePercentage",
    "renewablePercentage",
    "powerConsumptionTotal",
    "powerProductionTotal",
    "powerImportTotal",
    "powerExportTotal",
    "powerProductionBreakdown.nuclear",
    "powerProductionBreakdown.coal",
    "powerProductionBreakdown.gas",
    "powerProductionBreakdown.wind",
    "powerProductionBreakdown.solar",
    "powerProductionBreakdown.hydro",
]
carbon_intensity_essential_columns = [
    "zone",
    "datetime",
    "carbonIntensity",
    "isEstimated",
]


class ElectricityMapsDataTransformer:
    """
    Transform raw data from the ElectricityMap API.
    """

    def __init__(self, raw_data_folder: str, db_path: str):
        """
        Initialize the transformer with paths to raw and processed data.
        args:
            raw_data_folder (str): Path to the folder containing raw data.
            processed_data_folder (str): Path to the folder for saving processed data.
        """

        self.raw_data_folder = raw_data_folder
        self.db_path = db_path
        db_folder = os.path.dirname(self.db_path)
        if not os.path.exists(db_folder):
            os.makedirs(db_folder)

    def load_data(self, carbon_intensity_file: str, power_breakdown_file: str) -> tuple:
        """
        Load raw data from JSON files.
        args:
            carbon_intensity_file (str): Path to the carbon intensity file.
            power_breakdown_file (str): Path to the power breakdown file.
        """
        with open(carbon_intensity_file, "r") as f:
            carbon_intensity_raw_data = json.load(f)
        with open(power_breakdown_file, "r") as f:
            power_breakdown_raw_data = json.load(f)
        logger.info("Raw data loaded")
        return pd.json_normalize(
            carbon_intensity_raw_data["history"]
        ), pd.json_normalize(power_breakdown_raw_data["history"])

    def transform(
        self, carbon_intensity_df: pd.DataFrame, power_breakdown_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Transform raw data into a unified DataFrame.
        args:
            carbon_intensity_df (pd.DataFrame): Carbon intensity data.
            power_breakdown_df (pd.DataFrame): Power breakdown data.
        returns:
            pd.DataFrame: Unified DataFrame with essential columns.
        """
        carbon_intensity_df = carbon_intensity_df[
            carbon_intensity_essential_columns
        ].copy()
        power_breakdown_df = power_breakdown_df[
            power_breakdown_essential_columns
        ].copy()

        for df in [carbon_intensity_df, power_breakdown_df]:
            df["datetime"] = pd.to_datetime(df["datetime"])
        unified_df = pd.merge(
            carbon_intensity_df,
            power_breakdown_df,
            on=["zone", "datetime"],
            how="inner",
        )
        return unified_df

    def mark_as_processed(self, filename: str):
        """
        Mark a file as
        """
        backup_folder = os.path.join(self.raw_data_folder, "backup")
        if not os.path.exists(backup_folder):
            os.makedirs(backup_folder)
        shutil.move(filename, os.path.join(backup_folder, os.path.basename(filename)))
        logger.info(f"Moved {filename} to backup folder")

    def save_to_duckdb(self, df: pd.DataFrame):
        """
        Save the transformed DataFrame to DuckDB.
        Args:
            df (pd.DataFrame): The DataFrame to save.
        """
        with duckdb.connect(self.db_path) as conn:
            conn.register("temp_df", df)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS electricity_maps_data AS SELECT * FROM temp_df LIMIT 0"
            )
            conn.execute("INSERT INTO electricity_maps_data SELECT * FROM temp_df")

    def process_files(self, carbon_intensity_file: str, power_breakdown_file: str):
        """
        Process the raw data files.
        Args:
            carbon_intensity_file (str): Path to the carbon intensity file.
            power_breakdown_file (str): Path to the power breakdown file.
        """
        try:
            carbon_df, power_df = self.load_data(
                carbon_intensity_file=carbon_intensity_file,
                power_breakdown_file=power_breakdown_file,
            )
            df = self.transform(
                carbon_intensity_df=carbon_df, power_breakdown_df=power_df
            )
            self.save_to_duckdb(df=df)
            self.mark_as_processed(filename=carbon_intensity_file)
            self.mark_as_processed(filename=power_breakdown_file)
        except:
            logger.error(
                f"Error processing files: {carbon_intensity_file}, {power_breakdown_file}"
            )
            raise
        logger.info("Data processing complete")

    def process_all_files(self):
        """
        Iterate over all files in the raw data directory, process them, and save the results.
        """
        power_files = [
            f
            for f in os.listdir(self.raw_data_folder)
            if "power_breakdown" in f and f.endswith(".json")
        ]

        for power_file in power_files:

            power_file_path = os.path.join(self.raw_data_folder, power_file)
            carbon_file_path = os.path.join(
                self.raw_data_folder,
                power_file.replace("power_breakdown", "carbon_intensity"),
            )
            logger.info(f"Processing {power_file_path}")
            logger.info(f"Processing {carbon_file_path}")
            if not os.path.exists(carbon_file_path):
                logger.error(f"Missing carbon intensity file for {power_file}")
                continue
            self.process_files(
                carbon_intensity_file=carbon_file_path,
                power_breakdown_file=power_file_path,
            )
