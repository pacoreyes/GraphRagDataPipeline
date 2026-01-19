import pytest
import polars as pl
from data_pipeline.defs.checks import check_artist_index_integrity, check_genres_quality

def test_check_artist_index_integrity_pass():
    """Verify integrity check passes with clean data."""
    df = pl.DataFrame({
        "artist_uri": ["uri1", "uri2"],
        "name": ["Artist 1", "Artist 2"],
        "start_date": ["2020-01-01", "2021-01-01"]
    }).lazy()
    
    result = check_artist_index_integrity(df)
    assert result.passed

def test_check_artist_index_integrity_fail_duplicates():
    """Verify integrity check fails with duplicates."""
    df = pl.DataFrame({
        "artist_uri": ["uri1", "uri1"],
        "name": ["Artist 1", "Artist 1"],
        "start_date": ["2020-01-01", "2020-01-01"]
    }).lazy()
    
    result = check_artist_index_integrity(df)
    assert not result.passed

def test_check_genres_quality_pass():
    """Verify genre quality check passes."""
    df = pl.DataFrame({
        "name": ["Rock", "Pop"]
    }).lazy()
    result = check_genres_quality(df)
    assert result.passed
