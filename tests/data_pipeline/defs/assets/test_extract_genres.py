# -----------------------------------------------------------
# Unit Tests for genres
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
import polars as pl
from dagster import build_asset_context, MaterializeResult

from data_pipeline.defs.assets.extract_genres import extract_genres
from data_pipeline.utils.network_helpers import AsyncClient

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_genres.pl.scan_ndjson")
@patch("data_pipeline.defs.assets.extract_genres.async_append_jsonl")
@patch("data_pipeline.defs.assets.extract_genres.async_fetch_wikidata_entities_batch")
@patch("data_pipeline.defs.assets.extract_genres.settings")
@patch("data_pipeline.defs.assets.extract_genres.shutil.move")
async def test_extract_genres(
    mock_move,
    mock_settings,
    mock_fetch_entities,
    mock_append,
    mock_scan
):
    """
    Test the genres asset.
    """
    # Setup mock settings
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2
    
    # Mock Paths to control .exists() behavior
    temp_path_mock = MagicMock()
    final_path_mock = MagicMock()
    temp_path_mock.exists.return_value = True
    final_path_mock.exists.return_value = True # For row count check
    
    mock_settings.TEMP_DIRPATH.__truediv__.return_value = temp_path_mock
    mock_settings.DATASETS_DIRPATH.__truediv__.return_value = final_path_mock

    # Mock Input DataFrame
    mock_artists_df = pl.DataFrame({
        "genres": [["Q101", "Q102"], ["Q102", "Q103"], ["Q104"]]
    }).lazy()

    # Unique Genres Expected: Q101, Q102, Q103, Q104

    from contextlib import asynccontextmanager

    # Mock Context and Resource
    context = build_asset_context()
    mock_client = MagicMock(spec=AsyncClient)

    mock_wikidata = MagicMock()
    @asynccontextmanager
    async def mock_yield(context):
        yield mock_client
    mock_wikidata.get_client = mock_yield

    # Configure for batching test
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 2

    async def side_effect_fetch(context, qids, client=None, **kwargs):
        # Helper to construct a simple parent claim
        def p279_claim(pid):
            return {
                "P279": [
                    {
                        "mainsnak": {
                            "snaktype": "value",
                            "datavalue": {
                                "type": "wikibase-entityid",
                                "value": {"id": pid}
                            }
                        }
                    }
                ]
            }

        data = {
            "Q101": {
                "labels": {"en": {"value": "Genre A"}}, 
                "aliases": {"en": [{"value": "Alias A1"}]},
                "claims": p279_claim("Q999")
            },
            "Q102": {
                "labels": {"en": {"value": "Genre B"}}, 
                "aliases": {},
                "claims": {}
            },
            "Q103": {
                "labels": {"en": {"value": "Genre C"}}, 
                "aliases": {},
                "claims": {}
            },
            "Q104": {
                "labels": {"en": {"value": "Genre D"}}, 
                "aliases": {"en": [{"value": "Alias D1"}, {"value": "Alias D2"}]},
                "claims": p279_claim("Q888")
            }
        }
        return {k: v for k, v in data.items() if k in qids}

    mock_fetch_entities.side_effect = side_effect_fetch

    # Mock Scan
    mock_scan.return_value = pl.DataFrame({"name": ["Genre A"]}).lazy()

    # Execution
    result = await extract_genres(context, mock_wikidata, mock_artists_df)

    assert isinstance(result, MaterializeResult)
    assert mock_append.called
    
    # Check if shutil.move was called correctly
    assert mock_move.called
    args, _ = mock_move.call_args
    assert args[0] == temp_path_mock
    assert args[1] == final_path_mock
    
    # Verify that the processing logic picked up parents
    first_call_args = mock_append.call_args_list[0]
    data_written = first_call_args[0][1]
    
    # Check Q101 data
    q101_record = next((r for r in data_written if r["id"] == "Q101"), None)
    if q101_record:
        assert q101_record["name"] == "Genre A"
        assert q101_record["parent_ids"] == ["Q999"]
