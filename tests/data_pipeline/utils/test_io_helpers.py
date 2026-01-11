# -----------------------------------------------------------
# Unit Tests for io_helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
from pathlib import Path
from data_pipeline.utils.io_helpers import (
    async_read_json_file,
    async_write_json_file,
    async_read_text_file,
    async_write_text_file,
)
@pytest.mark.asyncio
async def test_json_read_write(tmp_path: Path):
    test_file = tmp_path / "test.json"
    test_data = {"key": "value", "nested": [1, 2, 3]}
    
    # Test write
    await async_write_json_file(test_file, test_data)
    assert test_file.exists()
    
    # Test read
    read_data = await async_read_json_file(test_file)
    assert read_data == test_data

@pytest.mark.asyncio
async def test_text_read_write(tmp_path: Path):
    test_file = tmp_path / "test.txt"
    test_content = "Hello, World!\nThis is a test."
    
    # Test write
    await async_write_text_file(test_file, test_content)
    assert test_file.exists()
    
    # Test read
    read_content = await async_read_text_file(test_file)
    assert read_content == test_content

@pytest.mark.asyncio
async def test_read_non_existent_file(tmp_path: Path):
    non_existent = tmp_path / "missing.json"
    assert await async_read_json_file(non_existent) is None
    assert await async_read_text_file(non_existent) is None

@pytest.mark.asyncio
async def test_auto_mkdir(tmp_path: Path):
    nested_file = tmp_path / "subdir" / "another" / "test.txt"
    await async_write_text_file(nested_file, "content")
    assert nested_file.exists()
    assert nested_file.parent.exists()
