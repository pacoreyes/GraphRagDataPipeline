# ----------------------------------------------------------- 
# Unit Tests for wikipedia_articles
# Dagster Data pipeline for Structured and Unstructured Data
# 
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# ----------------------------------------------------------- 

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
import polars as pl
from dagster import build_asset_context, MaterializeResult

from data_pipeline.defs.assets.extract_wikipedia_articles import extract_wikipedia_articles
from data_pipeline.utils.network_helpers import AsyncClient

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_wikipedia_articles.settings")
@patch("data_pipeline.defs.assets.extract_wikipedia_articles.shutil.move")
@patch("data_pipeline.defs.assets.extract_wikipedia_articles.async_append_jsonl")
@patch("data_pipeline.defs.assets.extract_wikipedia_articles.async_clear_file")
@patch("data_pipeline.defs.assets.extract_wikipedia_articles.pl.scan_ndjson")
async def test_extract_wikipedia_articles_flow(
    mock_scan, mock_clear, mock_append, mock_move, mock_settings
):
    # Setup Mocks
    mock_settings.TEMP_DIRPATH = MagicMock()
    mock_settings.DATASETS_DIRPATH = MagicMock()
    temp_path_mock = MagicMock()
    final_path_mock = MagicMock()
    temp_path_mock.exists.return_value = True
    final_path_mock.exists.return_value = True
    
    mock_settings.TEMP_DIRPATH.__truediv__.return_value = temp_path_mock
    mock_settings.DATASETS_DIRPATH.__truediv__.return_value = final_path_mock
    
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_ACTION_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2
    mock_settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY = 0
    mock_settings.WIKIPEDIA_CONCURRENT_REQUESTS = 2
    mock_settings.WIKIPEDIA_RATE_LIMIT_DELAY = 0
    mock_settings.WIKIPEDIA_API_URL = "http://wiki.api"
    mock_settings.WIKIDATA_ACTION_API_URL = "http://wd.api"
    mock_settings.MIN_CONTENT_LENGTH = 10
    mock_settings.ARTICLES_BUFFER_SIZE = 10
    mock_settings.WIKIPEDIA_CACHE_DIRPATH = Path("/tmp/wiki_cache")
    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}
    mock_settings.DEFAULT_EMBEDDINGS_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
    mock_settings.TEXT_CHUNK_SIZE = 100
    mock_settings.TEXT_CHUNK_OVERLAP = 10
    
    # Mock Data DataFrames (Input)
    artists_df = pl.DataFrame([
        {"id": "Q1", "name": "Artist One", "genres": ["QG1"]},
        {"id": "Q2", "name": "Artist Two", "genres": []}
    ])
    genres_df = pl.DataFrame([
        {"id": "QG1", "name": "Rock"}
    ])
    index_df = pl.DataFrame([
        {"artist_uri": "http://www.wikidata.org/entity/Q1", "start_date": "1991-01-01"}
    ])

    # Mock Wikidata Entity Fetch
    async def mock_fetch_entities(context, qids, client=None, **kwargs):
        result = {}
        if "Q1" in qids:
            result["Q1"] = {
                "sitelinks": {"enwiki": {"title": "Artist One"}}
            }
        if "Q2" in qids:
             result["Q2"] = {} # No sitelinks
        return result

    # Mock Dependencies
    with patch("data_pipeline.defs.assets.extract_wikipedia_articles.async_fetch_wikipedia_article", new_callable=AsyncMock) as mock_fetch, \
         patch("data_pipeline.defs.assets.extract_wikipedia_articles.async_fetch_wikidata_entities_batch", side_effect=mock_fetch_entities), \
         patch("data_pipeline.defs.assets.extract_wikipedia_articles.AutoTokenizer"), \
         patch("data_pipeline.defs.assets.extract_wikipedia_articles.RecursiveCharacterTextSplitter") as mock_splitter_cls:
        # Setup specific mock returns
        mock_fetch.return_value = "This is a sufficiently long text for Artist One to ensure it passes the minimal content filter of 50 characters."

        # Setup Text Splitter Mock
        mock_splitter_instance = mock_splitter_cls.from_huggingface_tokenizer.return_value
        mock_splitter_instance.split_text.return_value = ["Chunk 1", "Chunk 2"]

        from contextlib import asynccontextmanager

        # Create Context
        context = build_asset_context()
        mock_client = MagicMock(spec=AsyncClient)

        mock_wikidata = MagicMock()
        @asynccontextmanager
        async def mock_yield(context):
            yield mock_client
        mock_wikidata.get_client = mock_yield

        # Run Asset
        result = await extract_wikipedia_articles(
            context, 
            mock_wikidata, 
            artists_df.lazy(), 
            genres_df.lazy(), 
            index_df.lazy()
        )

        # Verifications
        assert isinstance(result, MaterializeResult)
        assert mock_move.called
        assert mock_append.called
        
        # Check content
        args = mock_append.call_args
        data_written = args[0][1] # buffer
        # We expect 2 chunks
        assert len(data_written) == 2
        
        # Verify Fetch called for valid artist only
        mock_fetch.assert_called_once()
        
        first_row = data_written[0]
        assert "id" in first_row
        assert first_row["id"] == "Q1_chunk_1"
        assert "metadata" in first_row
        assert first_row["metadata"]["artist_name"] == "Artist One"
        assert first_row["metadata"]["genres"] == ["Rock"]
        assert first_row["metadata"]["inception_year"] == 1991
        