# -----------------------------------------------------------
# Extract Tracks Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import asyncio
import shutil

import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext, MaterializeResult

from data_pipeline.models import Track, TRACK_SCHEMA
from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import AsyncClient
from data_pipeline.utils.io_helpers import async_append_jsonl, async_clear_file
from data_pipeline.utils.musicbrainz_helpers import (
    fetch_releases_for_group_async,
    fetch_tracks_for_release_async,
)
from data_pipeline.utils.data_transformation_helpers import normalize_and_clean_text
from data_pipeline.defs.resources import MusicBrainzResource


@asset(
    name="tracks",
    description="Extract Tracks dataset from the Releases list using MusicBrainz API.",
)
async def extract_tracks(
    context: AssetExecutionContext, 
    musicbrainz: MusicBrainzResource,
    releases: pl.LazyFrame
) -> MaterializeResult:
    """
    Retrieves all tracks for each release (Release Group) in the releases dataset from MusicBrainz.
    Strategy:
    1. Fetch all releases for the Release Group.
    2. Pick the earliest "Official" release.
    3. Fetch tracks for that specific Release.
    Returns a MaterializeResult with metadata, having moved the file to the final destination.
    """
    context.log.info("Starting tracks extraction from MusicBrainz.")

    # Temp file
    temp_file = settings.TEMP_DIRPATH / "tracks.jsonl"
    final_file = settings.DATASETS_DIRPATH / "tracks.jsonl"
    
    await async_clear_file(temp_file)

    # 1. Collect Release MBIDs
    # The 'id' in releases is the MBID of the Release Group.
    releases_df = releases.select(["id", "title"]).collect()
    
    rows = releases_df.to_dicts()
    total_releases = len(rows)

    if total_releases == 0:
        context.log.warning("No releases found. Returning empty tracks.")
        return MaterializeResult(
            metadata={
                "row_count": 0,
                "path": str(final_file),
                "sparse_json": True
            }
        )

    context.log.info(f"Found {total_releases} releases to process tracks for.")

    # 2. Worker Function
    buffer = []
    processed_count = 0
    
    async with musicbrainz.get_client(context) as client:
        for i, row in enumerate(rows):
            release_group_mbid = row["id"]
            release_title = row["title"]
            
            if i % 10 == 0:
                context.log.info(f"Processing tracks for release {i}/{total_releases}: {release_title}")

            # A. Find representative Release
            mb_releases = await fetch_releases_for_group_async(
                context=context,
                release_group_mbid=release_group_mbid,
                client=client,
                api_url=musicbrainz.api_url,
                headers=settings.DEFAULT_REQUEST_HEADERS,
                rate_limit_delay=musicbrainz.rate_limit_delay
            )
            
            if not mb_releases:
                continue

            # Selection Strategy: Prefer 'Official' status, then oldest date
            def sort_key(r):
                status_rank = 0 if r.get("status") == "Official" else 1
                date = r.get("date", "9999-99-99") or "9999-99-99"
                return (status_rank, date)

            mb_releases.sort(key=sort_key)
            best_release = mb_releases[0]
            release_id = best_release["id"]

            # B. Fetch Tracks for the chosen Release
            mb_tracks = await fetch_tracks_for_release_async(
                context=context,
                release_mbid=release_id,
                client=client,
                cache_dirpath=settings.MUSICBRAINZ_CACHE_DIRPATH,
                api_url=musicbrainz.api_url,
                headers=settings.DEFAULT_REQUEST_HEADERS,
                rate_limit_delay=musicbrainz.rate_limit_delay
            )
            
            for t in mb_tracks:
                track = Track(
                    id=t["id"],
                    title=normalize_and_clean_text(t["title"]),
                    album_id=release_group_mbid,
                )
                buffer.append(msgspec.to_builtins(track))
                
            # Flush buffer periodically
            if len(buffer) >= settings.TRACKS_BUFFER_SIZE:
                await async_append_jsonl(temp_file, buffer)
                processed_count += len(buffer)
                buffer = []
                
    # Flush remaining
    if buffer:
        await async_append_jsonl(temp_file, buffer)
        processed_count += len(buffer)

    context.log.info(f"Tracks extraction complete. Saved {processed_count} tracks to {temp_file}. Moving to {final_file}...")

    # 3. Move to Final Destination
    if await asyncio.to_thread(temp_file.exists):
        await asyncio.to_thread(shutil.move, str(temp_file), str(final_file))
    
    return MaterializeResult(
        metadata={
            "row_count": processed_count,
            "path": str(final_file),
            "sparse_json": True
        }
    )
