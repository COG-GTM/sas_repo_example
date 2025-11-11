"""
Configuration file for file paths and parameters.

This module centralizes path configurations for MEPS data files
and output locations to make the pipeline easy to configure.
"""

import os
from pathlib import Path


class MEPSPaths:
    """Configuration class for MEPS data file paths."""

    def __init__(self, base_dir=None):
        """
        Initialize MEPS paths configuration.

        Args:
            base_dir (str, optional): Base directory for MEPS data.
                                     Defaults to C:/MEPS on Windows or ~/MEPS on Unix.
        """
        if base_dir is None:
            if os.name == "nt":
                base_dir = "C:/MEPS"
            else:
                base_dir = str(Path.home() / "MEPS")

        self.base_dir = Path(base_dir)
        self.sas_dir = self.base_dir / "SAS" / "DATA"
        self.parquet_dir = self.base_dir / "parquet"
        self.output_dir = self.base_dir / "output"

    def get_h192_sas_path(self):
        """Get path to H192 SAS transport file."""
        return str(self.sas_dir / "h192.ssp")

    def get_h192_parquet_path(self):
        """Get path to H192 Parquet file."""
        return str(self.parquet_dir / "h192.parquet")

    def get_exercise_1a_output_path(self):
        """Get path for Exercise 1a output."""
        return str(self.output_dir / "exercise_1a_results.parquet")

    def get_exercise_1a_variance_output_path(self):
        """Get path for Exercise 1a variance estimation output."""
        return str(self.output_dir / "exercise_1a_variance.parquet")

    def ensure_directories(self):
        """Create all necessary directories if they don't exist."""
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)


DEFAULT_PATHS = MEPSPaths()


SPARK_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.autoBroadcastJoinThreshold": "10485760",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.files.maxPartitionBytes": "128MB",
}


SURVEY_DESIGN_VARS = {
    "stratum": "VARSTR",
    "psu": "VARPSU",
    "weight": "PERWT16F",
}
