import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from dagster import build_asset_context
from data_pipeline.utils.network_helpers import run_tasks_concurrently

@pytest.mark.asyncio
async def test_run_tasks_concurrently():
    """Verify that run_tasks_concurrently processes items and returns results."""
    items = [1, 2, 3]
    
    async def processor(item):
        return item * 2
        
    results = await run_tasks_concurrently(items, processor, concurrency_limit=2)
    assert results == [2, 4, 6]

@pytest.mark.asyncio
async def test_run_tasks_concurrently_empty():
    """Verify handling of empty item list."""
    results = await run_tasks_concurrently([], AsyncMock(), concurrency_limit=2)
    assert results == []
