# -----------------------------------------------------------
# Detect Communities Assets
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
Dagster assets for community detection and metadata aggregation.

Contains two assets:
1. detected_communities - Leiden community detection on the graph
2. community_metadata - Aggregated metadata per community
"""

from collections import Counter
from typing import Any

import polars as pl
from dagster import asset, AssetExecutionContext
from neo4j import Driver
from tqdm import tqdm

from data_pipeline.defs.resources import Neo4jResource
from data_pipeline.models import (
    COMMUNITY_ASSIGNMENT_SCHEMA,
    COMMUNITY_SCHEMA,
)
from data_pipeline.settings import settings
from data_pipeline.utils.neo4j_helpers import (
    build_igraph,
    run_leiden_multilevel,
    get_community_stats,
)


# =============================================================================
# Private Helper Functions
# =============================================================================

def _extract_graph_from_neo4j(
    driver: Driver,
    context: AssetExecutionContext,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[tuple[str, str]],
    list[tuple[str, str]],
]:
    """
    Extracts nodes and relationships from Neo4j for community detection.

    Args:
        driver: Neo4j driver instance.
        context: Dagster execution context.

    Returns:
        Tuple of (artists, genres, similar_to_edges, plays_genre_edges).
    """
    context.log.info("Extracting graph data from Neo4j...")

    with driver.session() as session:
        # Extract Artist nodes
        artists_result = session.run(
            "MATCH (a:Artist) RETURN a.id AS id, a.name AS name"
        )
        artists = [{"id": r["id"], "name": r["name"], "type": "artist"}
                   for r in artists_result]
        context.log.info(f"  Extracted {len(artists)} artists")

        # Extract Genre nodes
        genres_result = session.run(
            "MATCH (g:Genre) RETURN g.id AS id, g.name AS name"
        )
        genres = [{"id": r["id"], "name": r["name"], "type": "genre"}
                  for r in genres_result]
        context.log.info(f"  Extracted {len(genres)} genres")

        # Extract SIMILAR_TO relationships
        similar_result = session.run(
            "MATCH (a1:Artist)-[:SIMILAR_TO]->(a2:Artist) "
            "RETURN a1.id AS source, a2.id AS target"
        )
        similar_edges = [(r["source"], r["target"]) for r in similar_result]
        context.log.info(f"  Extracted {len(similar_edges)} SIMILAR_TO edges")

        # Extract PLAYS_GENRE relationships
        genre_result = session.run(
            "MATCH (a:Artist)-[:PLAYS_GENRE]->(g:Genre) "
            "RETURN a.id AS source, g.id AS target"
        )
        genre_edges = [(r["source"], r["target"]) for r in genre_result]
        context.log.info(f"  Extracted {len(genre_edges)} PLAYS_GENRE edges")

    return artists, genres, similar_edges, genre_edges


def _aggregate_list_column(values: list[list[str] | None], top_n: int = 10) -> list[str]:
    """
    Aggregates list of lists into top-N most frequent items.

    Args:
        values: List of string lists (may contain None).
        top_n: Number of top items to return.

    Returns:
        List of most frequent items, sorted by frequency descending.
    """
    counter: Counter[str] = Counter()
    for item_list in values:
        if item_list is not None:
            for item in item_list:
                if item:
                    counter[item] += 1
    return [item for item, _ in counter.most_common(top_n)]


def _aggregate_string_column(values: list[str | None], top_n: int = 5) -> list[str]:
    """
    Aggregates list of strings into top-N most frequent values.

    Args:
        values: List of strings (may contain None or empty strings).
        top_n: Number of top items to return.

    Returns:
        List of most frequent values, sorted by frequency descending.
    """
    counter: Counter[str] = Counter()
    for value in values:
        if value is not None and value != "":
            counter[value] += 1
    return [item for item, _ in counter.most_common(top_n)]


def _get_representative_artists(
    group_df: pl.DataFrame,
    top_n: int = 5,
) -> list[str]:
    """
    Selects representative artists based on similar_artists count.

    Artists with more connections (similar_artists) are considered
    more central/representative of the community.

    Args:
        group_df: DataFrame with artist data for a single community.
        top_n: Number of representative artists to return.

    Returns:
        List of artist names sorted by connectivity descending.
    """
    with_connectivity = group_df.with_columns(
        pl.col("similar_artists").list.len().fill_null(0).alias("connectivity")
    )
    top_artists = (
        with_connectivity.sort("connectivity", descending=True)
        .head(top_n)
        .select("artist_name")
        .to_series()
        .to_list()
    )
    return top_artists


# =============================================================================
# Asset 1: Community Assignments
# =============================================================================

@asset(
    name="detected_communities",
    description="Detects communities using Leiden algorithm on the music graph.",
    deps=["graph_db"],
)
def detect_communities(
    context: AssetExecutionContext,
    neo4j: Neo4jResource,
) -> pl.LazyFrame:
    """
    Runs Leiden community detection on the Neo4j graph.

    Extracts Artists, Genres, and their relationships, builds a bipartite
    graph, and runs Leiden at 3 resolution levels.

    Args:
        context: Dagster execution context.
        neo4j: Neo4j resource.

    Returns:
        LazyFrame with artist_id, artist_name, community_L0, L1, L2.
    """
    with neo4j.get_driver(context) as driver:
        # Extract graph data
        artists, genres, similar_edges, genre_edges = _extract_graph_from_neo4j(
            driver, context
        )

    # Combine all nodes
    all_nodes = artists + genres
    all_edges = similar_edges + genre_edges

    context.log.info(
        f"Building graph with {len(all_nodes)} nodes and {len(all_edges)} edges"
    )

    # Build igraph
    graph, id_to_idx = build_igraph(
        nodes=all_nodes,
        edges=all_edges,
        node_id_key="id",
        node_attrs=["name", "type"],
    )

    context.log.info(f"Graph built: {graph.vcount()} vertices, {graph.ecount()} edges")

    # Run Leiden at multiple resolutions
    context.log.info(f"Running Leiden with resolutions: {settings.LEIDEN_RESOLUTIONS}")
    memberships = run_leiden_multilevel(graph, settings.LEIDEN_RESOLUTIONS)

    # Log community statistics
    for i, membership in enumerate(memberships):
        stats = get_community_stats(membership)
        context.log.info(
            f"  Level {i}: {stats['num_communities']} communities, "
            f"largest={stats['largest']}, mean={stats['mean_size']:.1f}"
        )

    # Build DataFrame with only artists
    # Note: List accumulation acceptable here - dataset size is trivial (~4,700 artists)
    records = []
    for idx in range(graph.vcount()):
        if graph.vs[idx]["type"] == "artist":
            records.append({
                "artist_id": graph.vs[idx]["id"],
                "artist_name": graph.vs[idx]["name"],
                "community_L0": memberships[0][idx],
                "community_L1": memberships[1][idx],
                "community_L2": memberships[2][idx],
            })

    df = pl.DataFrame(records, schema=COMMUNITY_ASSIGNMENT_SCHEMA)
    context.log.info(f"Created assignments for {df.height} artists")

    return df.lazy()


# =============================================================================
# Asset 2: Community Metadata
# =============================================================================

@asset(
    name="community_metadata",
    description="Aggregates metadata for each detected community.",
)
def aggregate_community_metadata(
    context: AssetExecutionContext,
    detected_communities: pl.LazyFrame,
    artists: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Aggregates member metadata for each community at each level.

    Computes top tags, genres, countries, and representative artists.

    Args:
        context: Dagster execution context.
        detected_communities: LazyFrame with community assignments.
        artists: LazyFrame with artist metadata.

    Returns:
        LazyFrame with aggregated community metadata.
    """
    # Materialize and join
    assignments_df = detected_communities.collect()
    artists_df = artists.collect()

    context.log.info(
        f"Joining {assignments_df.height} assignments with {artists_df.height} artists"
    )

    joined = assignments_df.join(
        artists_df.select(["id", "country", "genres", "tags", "similar_artists"]),
        left_on="artist_id",
        right_on="id",
        how="left",
    )

    # Note: List accumulation acceptable here - dataset size is trivial (~600 communities)
    records = []
    total_communities = 0

    for level in range(3):
        level_col = f"community_L{level}"
        unique_communities = joined[level_col].unique().sort().to_list()
        total_communities += len(unique_communities)

        context.log.info(
            f"Processing Level {level}: {len(unique_communities)} communities"
        )

        for community_id in tqdm(
            unique_communities, desc=f"Level {level}", leave=False
        ):
            group_df = joined.filter(pl.col(level_col) == community_id)
            member_count = group_df.height

            # Aggregate metadata
            top_tags = _aggregate_list_column(group_df["tags"].to_list(), top_n=10)
            top_genres = _aggregate_list_column(group_df["genres"].to_list(), top_n=5)
            top_countries = _aggregate_string_column(
                group_df["country"].to_list(), top_n=3
            )
            representative_artists = _get_representative_artists(group_df, top_n=5)
            member_ids = group_df["artist_id"].to_list()

            records.append({
                "community_id": community_id,
                "level": level,
                "entity_type": "community",
                "member_count": member_count,
                "top_tags": top_tags,
                "top_genres": top_genres,
                "top_countries": top_countries,
                "representative_artists": representative_artists,
                "member_ids": member_ids,
            })

    df = pl.DataFrame(records, schema=COMMUNITY_SCHEMA)
    context.log.info(f"Created metadata for {total_communities} communities")

    return df.lazy()
