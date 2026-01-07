# -----------------------------------------------------------
# Dagster I/O Managers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import os
from pathlib import Path
from typing import Any, Union

import polars as pl
from dagster import ConfigurableIOManager, InputContext, OutputContext

from data_pipeline.settings import settings


class PolarsJSONLIOManager(ConfigurableIOManager):
    """
    I/O Manager that saves and loads Polars DataFrames as JSONL (newline-delimited JSON) files.
    """
    base_dir: str = str(settings.datasets_dirpath)

    def _get_path(self, context: Union[InputContext, OutputContext]) -> Path:
        """Determines the file path based on the asset key and partition."""
        asset_name = context.asset_key.path[-1]
        
        if context.has_partition_key:
            # Partitioned asset: data_volume/datasets/asset_name/partition_key.jsonl
            partition_key = context.partition_key
            return Path(self.base_dir) / asset_name / f"{partition_key}.jsonl"
        
        # Unpartitioned asset: data_volume/datasets/asset_name.jsonl
        return Path(self.base_dir) / f"{asset_name}.jsonl"

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        """Saves a Polars DataFrame to a JSONL file."""
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(f"Expected pl.DataFrame, got {type(obj)}")

        path = self._get_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        context.log.info(f"Writing {len(obj)} rows to {path}")
        obj.write_ndjson(path)
        
        # Add metadata to the asset in Dagster UI
        context.add_output_metadata({
            "row_count": len(obj),
            "path": str(path),
            "file_size_kb": os.path.getsize(path) / 1024
        })

    def load_input(self, context: InputContext) -> pl.DataFrame:
        """Loads a Polars DataFrame from a JSONL file."""
        path = self._get_path(context)
        context.log.info(f"Loading Polars DataFrame from {path}")
        
        if not path.exists():
            context.log.warning(f"File {path} does not exist. Returning empty DataFrame.")
            return pl.DataFrame()

        return pl.read_ndjson(path)
