# -----------------------------------------------------------
# Generate Community Summaries Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
Dagster asset for generating LLM summaries for communities.

Uses Qwen2.5-14B-Instruct via MLX for local inference to generate
natural language summaries describing each community's musical identity.
"""

from typing import Any

import polars as pl
from dagster import asset, AssetExecutionContext
from tqdm import tqdm

from data_pipeline.models import COMMUNITY_SCHEMA
from data_pipeline.settings import settings
from data_pipeline.utils.llm_helpers import load_mlx_model, generate_text


def _generate_community_name(row: dict[str, Any]) -> str:
    """
    Generates a human-readable name for a community.

    Combines top genre and country to create a descriptive name.
    Example: "German Techno" or "French House" or "UK Drum and Bass"

    Args:
        row: Dictionary with community metadata.

    Returns:
        Human-readable community name.
    """
    genres = row.get("top_genres") or []
    countries = row.get("top_countries") or []

    genre_part = genres[0] if genres else "Electronic"
    country_part = countries[0] if countries else ""

    if country_part:
        return f"{country_part} {genre_part}"
    return genre_part


def _build_summary_prompt(row: dict[str, Any]) -> str:
    """
    Builds the LLM prompt for community summarization.

    Args:
        row: Dictionary with community metadata fields.

    Returns:
        Formatted prompt string for the LLM.
    """
    genres = ", ".join(row["top_genres"]) if row["top_genres"] else "various genres"
    tags = ", ".join(row["top_tags"][:7]) if row["top_tags"] else "various styles"
    countries = (
        ", ".join(row["top_countries"]) if row["top_countries"] else "various countries"
    )
    artists = (
        ", ".join(row["representative_artists"][:4])
        if row["representative_artists"]
        else "various artists"
    )
    member_count = row["member_count"]

    return f"""You are a music historian and critic. Write a concise 2-3 sentence summary describing this community of electronic music artists.

Community details:
- Number of artists: {member_count}
- Primary genres: {genres}
- Musical characteristics/tags: {tags}
- Geographic origins: {countries}
- Representative artists: {artists}

Write a summary that captures the musical identity, stylistic characteristics, and cultural significance of this community. Be specific about the sound and style. Do not use bullet points."""


@asset(
    name="community_summaries",
    description="Generates LLM summaries for each community using MLX.",
)
def generate_community_summaries(
    context: AssetExecutionContext,
    community_metadata: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Generates natural language summaries for each community.

    Uses Qwen2.5-14B-Instruct via MLX for local inference.

    Args:
        context: Dagster execution context.
        community_metadata: LazyFrame with community metadata.

    Returns:
        LazyFrame with summaries added.
    """
    metadata_df = community_metadata.collect()
    total = metadata_df.height

    context.log.info(f"Loading MLX model: {settings.MLX_MODEL_PATH}")
    model, tokenizer = load_mlx_model(settings.MLX_MODEL_PATH)
    context.log.info("Model loaded successfully")

    context.log.info(f"Generating summaries for {total} communities...")

    # Note: List accumulation acceptable here - dataset size is trivial (~600 communities)
    records = []
    for row in tqdm(metadata_df.iter_rows(named=True), total=total, desc="Generating"):
        prompt = _build_summary_prompt(row)
        summary = generate_text(model, tokenizer, prompt, max_tokens=settings.MLX_MAX_TOKENS)
        name = _generate_community_name(row)

        records.append({
            "community_id": row["community_id"],
            "level": row["level"],
            "entity_type": "community",
            "member_count": row["member_count"],
            "top_tags": row["top_tags"],
            "top_genres": row["top_genres"],
            "top_countries": row["top_countries"],
            "representative_artists": row["representative_artists"],
            "member_ids": row["member_ids"],
            "name": name,
            "summary": summary,
        })

    df = pl.DataFrame(records, schema=COMMUNITY_SCHEMA)
    context.log.info(f"Generated {df.height} summaries")

    # Log sample
    sample = df.filter(pl.col("level") == 0).head(1)
    if sample.height > 0:
        context.log.info(f"Sample summary: {sample['summary'][0][:200]}...")

    return df.lazy()
