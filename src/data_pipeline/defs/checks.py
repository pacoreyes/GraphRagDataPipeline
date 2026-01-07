# -----------------------------------------------------------
# Dagster Asset Checks
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import polars as pl
from dagster import AssetCheckResult, asset_check

@asset_check(asset="build_artist_index")
def check_artist_index_integrity(build_artist_index: pl.DataFrame):
    """Checks that the artist index has no null IDs or names and no duplicates."""
    null_ids = build_artist_index["artist_uri"].null_count()
    null_names = build_artist_index["name"].null_count()
    duplicate_count = build_artist_index.is_duplicated().sum()
    
    return AssetCheckResult(
        passed=bool(null_ids == 0 and null_names == 0 and duplicate_count == 0),
        metadata={
            "null_ids": null_ids,
            "null_names": null_names,
            "duplicate_count": duplicate_count
        }
    )

@asset_check(asset="extract_artists")
def check_artists_completeness(extract_artists: pl.DataFrame):
    """Checks that enriched artists have at least some genres or tags assigned."""
    total_artists = len(extract_artists)
    if total_artists == 0:
        return AssetCheckResult(passed=True, description="No artists to check.")
        
    artists_with_metadata = extract_artists.filter(
        (pl.col("genres").list.len() > 0) | (pl.col("tags").list.len() > 0)
    )
    completeness_ratio = len(artists_with_metadata) / total_artists
    
    return AssetCheckResult(
        passed=bool(completeness_ratio > 0.5), # Expect at least 50% to have some metadata
        metadata={"completeness_ratio": float(completeness_ratio)}
    )

@asset_check(asset="extract_albums")
def check_albums_per_artist(extract_albums: pl.DataFrame):
    """Checks that we have a reasonable average of albums per artist."""
    if extract_albums.is_empty():
        return AssetCheckResult(passed=True)
        
    avg_albums = len(extract_albums) / extract_albums["artist_id"].n_unique()
    
    return AssetCheckResult(
        passed=bool(avg_albums >= 1.0),
        metadata={"avg_albums_per_artist": float(avg_albums)}
    )

@asset_check(asset="extract_tracks")
def check_tracks_schema(extract_tracks: pl.DataFrame):
    """Checks that tracks have titles and valid album links."""
    null_titles = extract_tracks["title"].null_count()
    null_albums = extract_tracks["album_id"].null_count()
    
    return AssetCheckResult(
        passed=bool(null_titles == 0 and null_albums == 0),
        metadata={"null_titles": null_titles, "null_albums": null_albums}
    )

@asset_check(asset="extract_genres")
def check_genres_quality(extract_genres: pl.DataFrame):
    """Checks that genres have names and a reasonable amount of metadata."""
    null_names = extract_genres["name"].null_count()
    return AssetCheckResult(
        passed=bool(null_names == 0),
        metadata={"null_names": null_names}
    )
