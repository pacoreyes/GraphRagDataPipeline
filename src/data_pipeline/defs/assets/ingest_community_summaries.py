# -----------------------------------------------------------
# Ingest Community Summaries Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
Dagster asset for ingesting community summaries into ChromaDB.

Embeds community summaries and adds them to the vector database
with entity_type="community" for differentiation from artist/genre documents.
"""

from typing import Any, Iterator

import polars as pl
from dagster import asset, AssetExecutionContext, MaterializeResult
from tqdm import tqdm

from data_pipeline.defs.resources import ChromaDBResource
from data_pipeline.settings import settings
from data_pipeline.utils.chroma_helpers import NomicEmbeddingFunction, get_device


def _prepare_community_metadata(row: dict[str, Any]) -> dict[str, Any]:
    """
    Prepares metadata for ChromaDB from a Community row.

    Converts list fields to comma-separated strings for ChromaDB compatibility.
    Truncates member_ids and tags to respect metadata size limits.

    Args:
        row: Dictionary with Community model fields.

    Returns:
        Flattened metadata dictionary suitable for ChromaDB.
    """
    return {
        "entity_type": row["entity_type"],
        "level": row["level"],
        "community_id": row["community_id"],
        "name": row["name"] or "",
        "member_count": row["member_count"],
        "top_genres": ", ".join(row["top_genres"]) if row["top_genres"] else "",
        "top_tags": (
            ", ".join(row["top_tags"][:settings.COMMUNITY_MAX_TAGS_IN_METADATA])
            if row["top_tags"]
            else ""
        ),
        "top_countries": (
            ", ".join(row["top_countries"]) if row["top_countries"] else ""
        ),
        "representative_artists": (
            ", ".join(row["representative_artists"])
            if row["representative_artists"]
            else ""
        ),
        # Store subset of member IDs (ChromaDB metadata size limits)
        "member_ids": (
            ",".join(row["member_ids"][:settings.COMMUNITY_MAX_MEMBER_IDS_IN_METADATA])
            if row["member_ids"]
            else ""
        ),
    }


def _iter_batches(lf: pl.LazyFrame, batch_size: int) -> Iterator[pl.DataFrame]:
    """
    Yields batches from a LazyFrame.

    Args:
        lf: Polars LazyFrame to iterate.
        batch_size: Number of rows per batch.

    Yields:
        DataFrame batches.
    """
    offset = 0
    while True:
        batch = lf.slice(offset, batch_size).collect()
        if batch.is_empty():
            break
        yield batch
        offset += batch_size


@asset(
    name="community_embeddings",
    description="Embeds community summaries into ChromaDB vector database.",
    deps=["vector_db"],
)
def ingest_community_summaries(
    context: AssetExecutionContext,
    chromadb: ChromaDBResource,
    community_summaries: pl.LazyFrame,
) -> MaterializeResult:
    """
    Ingests community summaries into ChromaDB.

    Adds summaries to the existing collection with entity_type="community".

    Args:
        context: Dagster execution context.
        chromadb: ChromaDB resource.
        community_summaries: LazyFrame with community summaries.

    Returns:
        MaterializeResult with ingestion statistics.
    """
    device = get_device()
    context.log.info(f"Using compute device: {device}")

    total_rows = community_summaries.select(pl.len()).collect().item()
    if total_rows == 0:
        context.log.warning("No community summaries to ingest.")
        return MaterializeResult(
            metadata={"documents_processed": 0, "status": "empty_input"}
        )

    context.log.info(f"Total summaries to process: {total_rows}")

    embedding_fn = NomicEmbeddingFunction(
        model_name=chromadb.model_name,
        device=str(device),
    )

    documents_processed = 0

    with chromadb.get_collection(
        context, embedding_function=embedding_fn
    ) as collection:
        # Check existing community documents
        existing_results = collection.get(
            where={"entity_type": "community"},
            include=["metadatas"],
        )
        existing_ids = (
            set(existing_results["ids"]) if existing_results["ids"] else set()
        )
        context.log.info(f"Found {len(existing_ids)} existing community documents")

        total_batches = (total_rows + chromadb.batch_size - 1) // chromadb.batch_size

        with tqdm(
            total=total_batches, desc="Ingesting communities", unit="batch"
        ) as pbar:
            for batch_df in _iter_batches(community_summaries, chromadb.batch_size):
                ids = []
                documents = []
                metadatas = []

                for row in batch_df.iter_rows(named=True):
                    doc_id = f"community_L{row['level']}_{row['community_id']}"

                    # Skip if already exists
                    if doc_id in existing_ids:
                        continue

                    # Prepare document with search prefix
                    document_text = f"search_document: {row['summary']}"
                    metadata = _prepare_community_metadata(row)

                    ids.append(doc_id)
                    documents.append(document_text)
                    metadatas.append(metadata)

                if documents:
                    collection.upsert(
                        ids=ids,
                        documents=documents,
                        metadatas=metadatas,
                    )
                    documents_processed += len(documents)

                pbar.update(1)

        final_count = collection.count()
        community_count = len(
            collection.get(
                where={"entity_type": "community"},
                include=["metadatas"],
            )["ids"]
            or []
        )

        context.log.info(
            f"Ingestion complete. Processed: {documents_processed}, "
            f"Community docs: {community_count}, Collection total: {final_count}"
        )

    return MaterializeResult(
        metadata={
            "documents_processed": documents_processed,
            "community_documents_total": community_count,
            "collection_total": final_count,
            "status": "success",
        }
    )
