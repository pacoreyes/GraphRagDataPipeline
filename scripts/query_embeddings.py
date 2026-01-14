# ----------------------------------------------------------- 
# Query Embeddings Script
# Standalone Script for Querying ChromaDB
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# ----------------------------------------------------------- 

"""
Standalone script to query a ChromaDB collection.
Reuses core embedding logic for consistency.
"""

from data_pipeline.settings import settings
from data_pipeline.utils.chroma_helpers import (
    get_chroma_client,
    get_collection_with_embedding
)


def main() -> None:
    # Configuration
    db_path = settings.VECTOR_DB_DIRPATH
    collection_name = settings.DEFAULT_COLLECTION_NAME
    query_text = "How many albums did Depeche Mode release?"
    n_results = 5

    print(f"Connecting to ChromaDB at {db_path}...")
    try:
        client = get_chroma_client(db_path)
        collection, emb_fn = get_collection_with_embedding(client, collection_name)
    except Exception as e:
        print(f"Error initializing DB: {e}")
        return

    print(f"\nQuerying for: '{query_text}'")
    print("-" * 30)

    # Perform query
    # Note: embed_query adds the required "search_query:" prefix internally
    query_embedding = emb_fn.embed_query(query_text)
    
    results = collection.query(
        query_embeddings=query_embedding,
        n_results=n_results,
        include=["documents", "metadatas", "distances"]
    )

    if not results or not results.get("ids") or not results["ids"][0]:
        print("No results found.")
        return

    print("\nResults (sorted by distance - lower is better):")
    print("-" * 30)
    
    # Iterate through the first query result list
    ids = results["ids"][0]
    metadatas = results["metadatas"][0] if results.get("metadatas") else [{}] * len(ids)
    distances = results["distances"][0] if results.get("distances") else ["N/A"] * len(ids)
    documents = results["documents"][0] if results.get("documents") else ["N/A"] * len(ids)

    for i, doc_id in enumerate(ids):
        meta = metadatas[i] or {}
        dist = distances[i]
        snippet = documents[i][:200].replace("\n", " ") + "..."

        print(f"Result {i + 1}:")
        print(f"  - ID:       {doc_id}")
        print(f"  - Title:    {meta.get('title', 'N/A')}")
        print(f"  - Artist:   {meta.get('artist_name', 'N/A')}")
        print(f"  - Distance: {dist:.4f}" if isinstance(dist, float) else f"  - Distance: {dist}")
        print(f"  - Snippet:  {snippet}")
        print("-" * 30)


if __name__ == "__main__":
    main()
