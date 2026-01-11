# -----------------------------------------------------------
# Extract Tracks Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Any

import httpx
import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Track
from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import (
    yield_batches_concurrently,
    deduplicate_stream,
)
from data_pipeline.utils.wikidata_helpers import (
    fetch_sparql_query_async,
    get_sparql_binding_value,
)
from data_pipeline.utils.text_transformation_helpers import normalize_and_clean_text
from data_pipeline.defs.resources import WikidataResource


def get_tracks_by_albums_batch_query(album_qids: list[str]) -> str:
    """
    Builds a SPARQL query to fetch tracks for multiple albums in one request.
    Uses bidirectional logic: Album->Track (P658) OR Track->Album (P361).
    Implements explicit label fallback (English -> Any).

    Args:
        album_qids: List of Album Wikidata QIDs.

    Returns:
        A SPARQL query string.
    """
    values = " ".join([f"wd:{qid}" for qid in album_qids])
    return f"""
    SELECT DISTINCT ?album ?track ?trackLabel ?genre WHERE {{
      VALUES ?album {{ {values} }}
      {{ ?album wdt:P658 ?track. }}  # Forward: Album has tracklist containing track
      UNION
      {{ ?track wdt:P361 ?album. }}  # Reverse: Track is part of album
      
      OPTIONAL {{ ?track wdt:P136 ?genre. }}

      # Label Fallback: English -> Any
      OPTIONAL {{ ?track rdfs:label ?enLabel . FILTER(LANG(?enLabel) = "en") }}
      OPTIONAL {{ ?track rdfs:label ?anyLabel . }}
      BIND(COALESCE(?enLabel, ?anyLabel) AS ?trackLabel)
    }}
    """


import uuid
from typing import Any

import httpx
import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Track
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import async_append_jsonl
from data_pipeline.utils.wikidata_helpers import (
    fetch_sparql_query_async,
    get_sparql_binding_value,
)
from data_pipeline.utils.text_transformation_helpers import normalize_and_clean_text
from data_pipeline.defs.resources import WikidataResource


def get_tracks_by_albums_batch_query(album_qids: list[str]) -> str:
    """
    Builds a SPARQL query to fetch tracks for multiple albums in one request.
    Uses bidirectional logic: Album->Track (P658) OR Track->Album (P361).
    Implements explicit label fallback (English -> Any).

    Args:
        album_qids: List of Album Wikidata QIDs.

    Returns:
        A SPARQL query string.
    """
    values = " ".join([f"wd:{qid}" for qid in album_qids])
    return f"""
    SELECT DISTINCT ?album ?track ?trackLabel ?genre WHERE {{
      VALUES ?album {{ {values} }}
      {{ ?album wdt:P658 ?track. }}  # Forward: Album has tracklist containing track
      UNION
      {{ ?track wdt:P361 ?album. }}  # Reverse: Track is part of album
      
      OPTIONAL {{ ?track wdt:P136 ?genre. }}

      # Label Fallback: English -> Any
      OPTIONAL {{ ?track rdfs:label ?enLabel . FILTER(LANG(?enLabel) = "en") }}
      OPTIONAL {{ ?track rdfs:label ?anyLabel . }}
      BIND(COALESCE(?enLabel, ?anyLabel) AS ?trackLabel)
    }}
    """


@asset(
    name="tracks",
    description="Extract Tracks dataset from the Albums list using Wikidata SPARQL.",
)
async def extract_tracks(
    context: AssetExecutionContext, 
    wikidata: WikidataResource, 
    albums: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Retrieves all tracks for each album in the albums dataset from Wikidata.
    Returns a Polars LazyFrame backed by a temporary JSONL file.
    """
    context.log.info("Starting track extraction from albums dataset.")

    # Temp file
    temp_file = settings.DATASETS_DIRPATH / ".temp" / f"tracks_{uuid.uuid4()}.jsonl"
    temp_file.parent.mkdir(parents=True, exist_ok=True)

    # 1. Get Total Rows (Lazy Count)
    total_rows = albums.select(pl.len()).collect().item()
    if total_rows == 0:
        return pl.LazyFrame()

    context.log.info(f"Fetching tracks for {total_rows} albums.")

    # 2. Define worker function for batch processing
    async def process_batch(
        qid_chunk: list[str], client: httpx.AsyncClient
    ) -> list[dict[str, Any]]:
        query = get_tracks_by_albums_batch_query(qid_chunk)
        results = await fetch_sparql_query_async(
            context, 
            query, 
            sparql_endpoint=settings.WIKIDATA_SPARQL_ENDPOINT,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            timeout=settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT,
            client=client
        )

        # Map to aggregate aliases and deduplicate within batch
        # Key: (track_id, album_id) -> {title, genres}
        track_map: dict[tuple[str, str], dict[str, Any]] = {}

        for row in results:
            track_uri = get_sparql_binding_value(row, "track")
            album_uri = get_sparql_binding_value(row, "album")
            title = get_sparql_binding_value(row, "trackLabel")
            genre_uri = get_sparql_binding_value(row, "genre")

            if not all([track_uri, album_uri, title]):
                continue

            title = normalize_and_clean_text(title)
            track_id = track_uri.split("/")[-1]
            album_id = album_uri.split("/")[-1]

            key = (track_id, album_id)
            if key not in track_map:
                track_map[key] = {
                    "title": title,
                    "genres": set()
                }

            if genre_uri:
                genre_id = genre_uri.split("/")[-1]
                track_map[key]["genres"].add(genre_id)

        return [
            msgspec.to_builtins(
                Track(
                    id=tid,
                    title=data["title"],
                    album_id=aid,
                    genres=list(data["genres"]) if data["genres"] else None,
                )
            )
            for (tid, aid), data in track_map.items()
        ]

    # 3. Stream processing
    batch_size = settings.WIKIDATA_ACTION_BATCH_SIZE
    
    async with wikidata.get_client(context) as client:
        for offset in range(0, total_rows, batch_size):
            context.log.info(f"Processing tracks batch offset {offset}/{total_rows}")
            
            # Fetch batch of QIDs
            batch_df = albums.slice(offset, batch_size).select("id").collect()
            batch_qids = batch_df["id"].to_list()
            
            if not batch_qids:
                continue

            # Process
            tracks_batch = await process_batch(batch_qids, client)
            
            # Write
            await async_append_jsonl(temp_file, tracks_batch)

    context.log.info(f"Tracks extracted to {temp_file}")
    
    # Return LazyFrame with lazy deduplication
    return pl.scan_ndjson(str(temp_file)).unique(subset=["id", "album_id"])
