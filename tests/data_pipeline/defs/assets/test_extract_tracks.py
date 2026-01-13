import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch
from dagster import build_asset_context, MaterializeResult

from data_pipeline.defs.assets.extract_tracks import extract_tracks

@pytest.fixture

def mock_fetch_releases(mocker):

    return mocker.patch(

        "data_pipeline.defs.assets.extract_tracks.fetch_releases_for_group_async",

        new_callable=AsyncMock

    )



@pytest.fixture

def mock_fetch_tracks(mocker):

    return mocker.patch(

        "data_pipeline.defs.assets.extract_tracks.fetch_tracks_for_release_async",

        new_callable=AsyncMock

    )



@pytest.mark.asyncio

@patch("data_pipeline.defs.assets.extract_tracks.settings")

@patch("data_pipeline.defs.assets.extract_tracks.shutil.move")

@patch("data_pipeline.defs.assets.extract_tracks.async_append_jsonl")

@patch("data_pipeline.defs.assets.extract_tracks.async_clear_file")

@patch("data_pipeline.defs.assets.extract_tracks.pl.scan_ndjson")

async def test_extract_tracks_success(

    mock_scan, mock_clear, mock_append, mock_move, mock_settings, mock_fetch_releases, mock_fetch_tracks

):

    """

    Test that extract_tracks correctly processes releases and extracts tracks from MusicBrainz.

    """

    # Setup Mocks

    mock_settings.TEMP_DIRPATH = MagicMock()

    mock_settings.DATASETS_DIRPATH = MagicMock()

    mock_settings.MUSICBRAINZ_CACHE_DIRPATH = Path("/tmp/mb_cache")

    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}

    mock_settings.MUSICBRAINZ_REQUEST_TIMEOUT = 30

    mock_settings.MUSICBRAINZ_RATE_LIMIT_DELAY = 0

    mock_settings.MUSICBRAINZ_API_URL = "http://mb.api"

    mock_settings.TRACKS_BUFFER_SIZE = 100

    

    temp_path_mock = MagicMock()

    final_path_mock = MagicMock()

    temp_path_mock.exists.return_value = True

    final_path_mock.exists.return_value = True

    

    mock_settings.TEMP_DIRPATH.__truediv__.return_value = temp_path_mock

    mock_settings.DATASETS_DIRPATH.__truediv__.return_value = final_path_mock



    # 1. Setup Input Data

    releases_df = pl.DataFrame({

        "id": ["rg-123"],

        "title": ["Test Album"]

    }).lazy()



    # 2. Setup Mock Return Values

    mock_fetch_releases.return_value = [

        {"id": "rel-1", "status": "Official", "date": "2020-01-01"}

    ]

    mock_fetch_tracks.return_value = [

        {"id": "rec-1", "title": "Song A", "length": 100},

        {"id": "rec-2", "title": "Song B", "length": 200}

    ]



    # 3. Run Asset

    context = build_asset_context()

    result = await extract_tracks(context, releases_df)



    # 4. Verify

    assert isinstance(result, MaterializeResult)

    assert mock_move.called

    assert mock_append.called

    

    args = mock_append.call_args

    data_written = args[0][1]

    

    assert len(data_written) == 2

    assert data_written[0]["id"] == "rec-1"

    assert data_written[0]["album_id"] == "rg-123"

    assert data_written[1]["title"] == "Song B"



    assert mock_fetch_releases.call_count == 1

    _, kwargs_rel = mock_fetch_releases.call_args

    assert kwargs_rel["release_group_mbid"] == "rg-123"



    assert mock_fetch_tracks.call_count == 1

    _, kwargs_tr = mock_fetch_tracks.call_args

    assert kwargs_tr["release_mbid"] == "rel-1"



@pytest.mark.asyncio

@patch("data_pipeline.defs.assets.extract_tracks.settings")

@patch("data_pipeline.defs.assets.extract_tracks.shutil.move")

async def test_extract_tracks_empty_input(mock_move, mock_settings, mock_fetch_releases, mock_fetch_tracks):

    """

    Test handling of empty input dataframe.

    """

    mock_settings.TEMP_DIRPATH = MagicMock()

    mock_settings.DATASETS_DIRPATH = MagicMock()

    

    artists_df = pl.DataFrame(schema={"id": pl.Utf8, "title": pl.Utf8}).lazy()



    context = build_asset_context()

    result = await extract_tracks(context, artists_df)



    assert isinstance(result, MaterializeResult)

    assert result.metadata["row_count"] == 0

    mock_fetch_releases.assert_not_called()

    mock_fetch_tracks.assert_not_called()
