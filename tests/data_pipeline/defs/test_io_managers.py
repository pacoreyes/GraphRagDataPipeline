import pytest
import polars as pl
from dagster import build_input_context, build_output_context, AssetKey
from data_pipeline.defs.io_managers import PolarsParquetIOManager, PolarsJSONLIOManager

def test_parquet_io_manager_path():
    """Verify path generation for Parquet IO Manager."""
    manager = PolarsParquetIOManager(base_dir="/tmp/test", extension="parquet")
    context = build_output_context(asset_key=AssetKey("test_asset"))
    path = manager._get_path(context)
    assert str(path) == "/tmp/test/test_asset.parquet"

def test_jsonl_io_manager_path():
    """Verify path generation for JSONL IO Manager."""
    manager = PolarsJSONLIOManager(base_dir="/tmp/test", extension="jsonl")
    context = build_output_context(asset_key=AssetKey("test_asset"))
    path = manager._get_path(context)
    assert str(path) == "/tmp/test/test_asset.jsonl"

def test_io_manager_with_partition():
    """Verify path generation with partitions."""
    manager = PolarsParquetIOManager(base_dir="/tmp/test", extension="parquet")
    context = build_output_context(asset_key=AssetKey("test_asset"), partition_key="2020s")
    path = manager._get_path(context)
    assert str(path) == "/tmp/test/test_asset/2020s.parquet"
