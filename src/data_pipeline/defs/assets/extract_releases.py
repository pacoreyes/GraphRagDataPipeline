# -----------------------------------------------------------
# Extract Releases Asset (MusicBrainz)
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import uuid
import asyncio

import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Album
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import async_append_jsonl
from data_pipeline.utils.musicbrainz_helpers import get_artist_release_groups_filtered, get_release_group_details
from data_pipeline.utils.text_transformation_helpers import normalize_and_clean_text

@asset(
    name="releases",
    description="Extract Releases (Albums/Singles) from MusicBrainz.",
)
async def extract_releases(
    context: AssetExecutionContext, 
    artists: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Retrieves all filtered release groups (Albums/Singles) for each artist from MusicBrainz.
    Uses the 'mbid' from the artist dataset.
    Returns a Polars LazyFrame backed by a temporary JSONL file.
    """
    context.log.info("Starting releases extraction from MusicBrainz.")

    # Temp file
    temp_file = settings.DATASETS_DIRPATH / ".temp" / f"releases_{uuid.uuid4()}.jsonl"
    temp_file.parent.mkdir(parents=True, exist_ok=True)

    # 1. Collect Artist MBIDs
    # We need 'id' (QID) to link back, and 'mbid' to query MB.
    # Filter out artists without MBID.
    artists_df = artists.select(["id", "mbid", "name"]).filter(pl.col("mbid").is_not_null()).collect()
    
    rows = artists_df.to_dicts()
    total_artists = len(rows)
    
    if total_artists == 0:
        context.log.warning("No artists with MBIDs found. Returning empty releases.")
        return pl.LazyFrame()

    context.log.info(f"Found {total_artists} artists with MBIDs to process.")

    # 2. Worker Function
    # Since musicbrainzngs is blocking/sync, and we have a strict rate limit,
    # we process sequentially inside an async wrapper or use to_thread.
    # Given the strict 1 req/sec limit, massive parallelism isn't helpful.
    # We will process linearly to be safe and polite.
    
    buffer = []
    processed_count = 0
    
    for i, row in enumerate(rows):
        artist_qid = row["id"]
        artist_mbid = row["mbid"]
        artist_name = row["name"]
        
        # Log progress every 10 artists
        if i % 10 == 0:
            context.log.info(f"Processing artist {i}/{total_artists}: {artist_name}")

        # Fetch Release Groups (Sync call wrapped in thread to not block event loop)
        rgs = await asyncio.to_thread(get_artist_release_groups_filtered, artist_mbid)
        
        for rg in rgs:
            rg_id = rg["id"]
            title = rg["title"]
            first_release_date = rg.get("first-release-date", "")
            
            # Parse Year
            year = None
            if first_release_date:
                try:
                    year = int(first_release_date.split("-")[0])
                except (ValueError, IndexError):
                    pass
            
            # Note: We are NOT calling get_release_group_details here for every album
            # because that would explode the runtime (e.g. 50k requests).
            # We rely on the browse data.
            
            album = Album(
                id=rg_id,  # MusicBrainz ID
                title=normalize_and_clean_text(title),
                year=year,
                artist_id=artist_qid, # Link to Graph QID
                genres=None # Browse doesn't give genres
            )
            buffer.append(msgspec.to_builtins(album))
            
        # Flush buffer periodically
        if len(buffer) >= 100:
            await async_append_jsonl(temp_file, buffer)
            processed_count += len(buffer)
            buffer = []
            
    # Flush remaining
    if buffer:
        await async_append_jsonl(temp_file, buffer)
        processed_count += len(buffer)

    context.log.info(f"Extraction complete. Saved {processed_count} releases to {temp_file}")
    
    return pl.scan_ndjson(str(temp_file))
