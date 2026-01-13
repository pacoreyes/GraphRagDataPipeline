# -----------------------------------------------------------
# MusicBrainz API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext

from data_pipeline.utils.network_helpers import (
    make_async_request_with_retries,
    AsyncClient,
)
from data_pipeline.utils.io_helpers import async_read_json_file, async_write_json_file


async def fetch_artist_release_groups_async(
    context: AssetExecutionContext, 
    artist_mbid: str, 
    client: AsyncClient,
    cache_dirpath: Path,
    api_url: str,
    headers: dict[str, str],
    rate_limit_delay: float = 1.0,
) -> list[dict[str, Any]]:
    """
    Fetches all release groups for an artist from MusicBrainz.
    Handles pagination automatically. Implements local JSON caching.
    Uses AsyncClient and exponential backoff retries.
    """
    cache_file = cache_dirpath / f"{artist_mbid}_release.json"
    all_release_groups = []

    # 1. Check Cache
    cached_data = await async_read_json_file(cache_file)
    if cached_data is not None:
        return cached_data

    # 2. Fetch from API if not cached
    limit = 100
    offset = 0
    url = f"{api_url}/release-group"
    
    try:
        while True:
            params = {
                "artist": artist_mbid,
                "limit": limit,
                "offset": offset,
                "fmt": "json"
            }
            
            response = await make_async_request_with_retries(
                context=context,
                url=url,
                method="GET",
                params=params,
                headers=headers,
                client=client,
                rate_limit_delay=rate_limit_delay
            )
            
            data = response.json()
            batch = data.get('release-groups', [])
            all_release_groups.extend(batch)
            
            if len(batch) < limit:
                break
                
            offset += limit
        
        # Save to cache
        await async_write_json_file(cache_file, all_release_groups)
            
    except Exception as e:
        context.log.error(f"MusicBrainz API Async Error for {artist_mbid}: {e}")
        return []

    return all_release_groups


async def fetch_releases_for_group_async(
    context: AssetExecutionContext,
    release_group_mbid: str,
    client: AsyncClient,
    api_url: str,
    headers: dict[str, str],
    rate_limit_delay: float = 1.0,
) -> list[dict[str, Any]]:
    """
    Fetches the list of releases associated with a Release Group.
    """
    url = f"{api_url}/release-group/{release_group_mbid}"
    params = {"inc": "releases", "fmt": "json"}
    
    try:
        response = await make_async_request_with_retries(
            context=context,
            url=url,
            method="GET",
            params=params,
            headers=headers,
            client=client,
            rate_limit_delay=rate_limit_delay
        )
        data = response.json()
        return data.get("releases", [])
    except Exception as e:
        context.log.error(f"Error fetching releases for group {release_group_mbid}: {e}")
        return []


async def fetch_tracks_for_release_async(
    context: AssetExecutionContext,
    release_mbid: str,
    client: AsyncClient,
    cache_dirpath: Path,
    api_url: str,
    headers: dict[str, str],
    rate_limit_delay: float = 1.0,
) -> list[dict[str, Any]]:
    """
    Fetches the tracklist for a specific Release MBID.
    """
    cache_file = cache_dirpath / f"{release_mbid}_tracks.json"
    
    # 1. Check Cache
    cached_tracks = await async_read_json_file(cache_file)
    if cached_tracks is not None:
        return cached_tracks

    # 2. Fetch from API
    url = f"{api_url}/release/{release_mbid}"
    params = {"inc": "recordings", "fmt": "json"}
    
    try:
        response = await make_async_request_with_retries(
            context=context,
            url=url,
            method="GET",
            params=params,
            headers=headers,
            client=client,
            rate_limit_delay=rate_limit_delay
        )
        data = response.json()
        
        tracks = []
        for medium in data.get("media", []):
            for track in medium.get("tracks", []):
                recording = track.get("recording", {})
                if not recording:
                    continue
                    
                tracks.append({
                    "id": recording["id"],
                    "title": recording["title"],
                    "length": recording.get("length")
                })
        
        await async_write_json_file(cache_file, tracks)
        return tracks

    except Exception as e:
        context.log.error(f"Error fetching tracks for release {release_mbid}: {e}")
        return []
