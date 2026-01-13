import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch
from dagster import AssetExecutionContext, build_asset_context, MaterializeResult

from data_pipeline.defs.assets.extract_releases import extract_releases

# Mock the helper function to avoid network calls
@pytest.fixture
def mock_fetch_release_groups(mocker):
    return mocker.patch(
        "data_pipeline.defs.assets.extract_releases.fetch_artist_release_groups_async",
        new_callable=AsyncMock
    )

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_releases.settings")
@patch("data_pipeline.defs.assets.extract_releases.shutil.move")
@patch("data_pipeline.defs.assets.extract_releases.async_append_jsonl")
@patch("data_pipeline.defs.assets.extract_releases.async_clear_file")
@patch("data_pipeline.defs.assets.extract_releases.pl.scan_ndjson")
async def test_extract_releases_success(
    mock_scan, mock_clear, mock_append, mock_move, mock_settings, mock_fetch_release_groups
):
    """
    Test that extract_releases correctly processes artists and extracts releases.
    """
    # Setup Mocks
    mock_settings.TEMP_DIRPATH = MagicMock()
    mock_settings.DATASETS_DIRPATH = MagicMock()
    mock_settings.MUSICBRAINZ_CACHE_DIRPATH = Path("/tmp/mb_cache")
    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}
    mock_settings.MUSICBRAINZ_REQUEST_TIMEOUT = 30
    mock_settings.MUSICBRAINZ_RATE_LIMIT_DELAY = 0
    mock_settings.MUSICBRAINZ_API_URL = "http://mb.api"
    mock_settings.RELEASES_BUFFER_SIZE = 100
    
    temp_path_mock = MagicMock()
    final_path_mock = MagicMock()
    temp_path_mock.exists.return_value = True
    final_path_mock.exists.return_value = True
    
    mock_settings.TEMP_DIRPATH.__truediv__.return_value = temp_path_mock
    mock_settings.DATASETS_DIRPATH.__truediv__.return_value = final_path_mock

    # 1. Setup Input Data
    # Artist with valid MBID
    artists_df = pl.DataFrame({
        "id": ["Q123"],
        "mbid": ["mbid-123"],
        "name": ["Test Artist"]
    }).lazy()

    # 2. Setup Mock Return Value
    # Return two releases: one valid Album, one Single (handled by logic)
    # The helper already filters for Album/Single, so we just return what the helper would return.
    mock_fetch_release_groups.return_value = [
        {
            "id": "rel-1",
            "title": "Test Album",
            "first-release-date": "2020-01-01",
            "primary-type": "Album"
        },
        {
            "id": "rel-2",
            "title": "Test Single",
            "first-release-date": "2021",
            "primary-type": "Single"
        }
    ]

    # 3. Create Context
    context = build_asset_context()

    # 4. Run Asset
    result = await extract_releases(context, artists_df)

    # 5. Verify Results
    assert isinstance(result, MaterializeResult)
    assert mock_move.called
    assert mock_append.called
    
    # Verify content written
    args = mock_append.call_args
    data_written = args[0][1] # list of dicts
    assert len(data_written) == 2
    
    # Check Row 1
    row1 = data_written[0]
    assert row1["id"] == "rel-1"
    assert row1["title"] == "Test Album"
    assert row1["year"] == 2020
    assert row1["artist_id"] == "Q123"

    # Check Row 2
    row2 = data_written[1]
    assert row2["id"] == "rel-2"
    assert row2["year"] == 2021
    
    # Verify mock was called correctly
    assert mock_fetch_release_groups.call_count == 1
    _, kwargs = mock_fetch_release_groups.call_args
    assert kwargs["artist_mbid"] == "mbid-123"
    
@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_releases.settings")
@patch("data_pipeline.defs.assets.extract_releases.shutil.move")
async def test_extract_releases_empty_input(mock_move, mock_settings, mock_fetch_release_groups):
    """
    Test handling of empty input dataframe.
    """
    mock_settings.TEMP_DIRPATH = MagicMock()
    mock_settings.DATASETS_DIRPATH = MagicMock()
    
    artists_df = pl.DataFrame(schema={"id": pl.Utf8, "mbid": pl.Utf8, "name": pl.Utf8}).lazy()

    context = build_asset_context()
    result = await extract_releases(context, artists_df)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["row_count"] == 0
    mock_fetch_release_groups.assert_not_called()

