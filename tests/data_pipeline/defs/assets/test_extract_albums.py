# -----------------------------------------------------------
# Unit Tests for albums
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
import httpx
from pathlib import Path
from unittest.mock import patch, MagicMock
import polars as pl
from dagster import build_asset_context

from data_pipeline.defs.assets.extract_albums import extract_albums

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_albums.pl.scan_ndjson")
@patch("data_pipeline.defs.assets.extract_albums.async_append_jsonl")
@patch("data_pipeline.defs.assets.extract_albums.fetch_sparql_query_async")
@patch("data_pipeline.defs.assets.extract_albums.settings")
async def test_extract_albums(
    mock_settings,
    mock_fetch_sparql,
    mock_append,
    mock_scan
):
    """
    Test the albums asset.
    """
    # Setup mock settings
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2
    mock_settings.DATASETS_DIRPATH = Path("/tmp")

    # Mock Input DataFrame (artists)
    mock_artists_df = pl.DataFrame({
        "id": ["Q1", "Q2", "Q3"]
    }).lazy()

    # Mock SPARQL Response
    mock_fetch_sparql.return_value = [
        {
            "album": {"value": "http://www.wikidata.org/entity/QA"},
            "artist": {"value": "http://www.wikidata.org/entity/Q1"},
            "albumLabel": {"value": "Album A"},
            "releaseDate": {"value": "2010-01-01T00:00:00Z"},
        },
        {
            "album": {"value": "http://www.wikidata.org/entity/QA"},
            "artist": {"value": "http://www.wikidata.org/entity/Q1"},
            "albumLabel": {"value": "Album A"},
            "releaseDate": {"value": "2009-05-05T00:00:00Z"} # Earlier date
        },
        {
            "album": {"value": "http://www.wikidata.org/entity/QB"},
            "artist": {"value": "http://www.wikidata.org/entity/Q2"},
            "albumLabel": {"value": "Album B"},
            "releaseDate": {"value": "2020-10-10T00:00:00Z"}
        },
        # Duplicates for Q3
        {
            "album": {"value": "http://www.wikidata.org/entity/Q3A"},
            "artist": {"value": "http://www.wikidata.org/entity/Q3"},
            "albumLabel": {"value": "Dup Title"},
            "releaseDate": {"value": "2015-01-01T00:00:00Z"}
        },
        {
            "album": {"value": "http://www.wikidata.org/entity/Q3B"},
            "artist": {"value": "http://www.wikidata.org/entity/Q3"},
            "albumLabel": {"value": "Dup Title"},
            "releaseDate": {"value": "2005-01-01T00:00:00Z"}
        }
    ]
    
    # Mock Scan Result
    mock_scan.return_value = pl.DataFrame({"title": ["Album A"]}).lazy()

    from contextlib import asynccontextmanager

    # Mock Context and Resource
    context = build_asset_context()
    mock_client = MagicMock(spec=httpx.AsyncClient)

    mock_wikidata = MagicMock()
    @asynccontextmanager
    async def mock_yield(context):
        yield mock_client
    mock_wikidata.get_client = mock_yield

    # Execution
    result_df = await extract_albums(context, mock_wikidata, mock_artists_df)

    assert isinstance(result_df, pl.LazyFrame)
    assert mock_append.called
    assert mock_scan.called