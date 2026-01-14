# ----------------------------------------------------------- 
# Generate Embeddings Visualization Script
# Standalone Script for Vector Space Visualization
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# ----------------------------------------------------------- 

import chromadb
import nomic
import numpy as np
from nomic import atlas
from nomic.data_inference import NomicTopicOptions
from tqdm import tqdm

from data_pipeline.settings import settings
from data_pipeline.utils.chroma_helpers import get_chroma_client


def fetch_all_embeddings(collection: chromadb.Collection, batch_size: int = 500) -> dict:
    """
    Fetches all embeddings and metadata from a ChromaDB collection in batches.
    """
    total_docs = collection.count()
    if total_docs == 0:
        return {}

    print(f"Fetching {total_docs} documents from collection '{collection.name}'...")
    
    all_results = {"ids": [], "embeddings": [], "metadatas": []}

    with tqdm(total=total_docs, desc="Fetching documents") as pbar:
        for offset in range(0, total_docs, batch_size):
            batch = collection.get(
                limit=min(batch_size, total_docs - offset),
                offset=offset,
                include=["embeddings", "metadatas"],
            )
            if batch["ids"] and len(batch["ids"]) > 0:
                all_results["ids"].extend(batch["ids"])
            if batch["embeddings"] is not None and len(batch["embeddings"]) > 0:
                all_results["embeddings"].extend(batch["embeddings"])
            if batch["metadatas"] and len(batch["metadatas"]) > 0:
                all_results["metadatas"].extend(batch["metadatas"])
            pbar.update(len(batch["ids"]) if batch["ids"] else 0)
            
    return all_results


def main() -> None:
    """
    Main execution function to generate the Nomic Atlas visualization.
    """
    print("--- ChromaDB Vector Space Visualization ---")

    # 1. Authenticate with Nomic
    if settings.NOMIC_API_KEY:
        nomic.login(settings.NOMIC_API_KEY)
    else:
        print("Warning: NOMIC_API_KEY not found in settings. Visualization upload may fail.")

    collection_name = settings.DEFAULT_COLLECTION_NAME
    project_name = "GraphRAG Electronic Music Visualization"

    # 2. Connect using helper
    try:
        client = get_chroma_client(settings.VECTOR_DB_DIRPATH)
        collection = client.get_collection(name=collection_name)
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return

    # 3. Fetch Data
    data = fetch_all_embeddings(collection)
    if not data or not data.get("embeddings") or len(data["embeddings"]) == 0:
        print("Error: No data found to visualize.")
        return

    # 4. Prepare Data for Nomic Atlas
    print("Preparing data for Nomic Atlas...")
    embeddings = np.array(data["embeddings"])
    metadata = data["metadatas"]

    # Ensure each record has an ID for Nomic
    for i, doc_id in enumerate(data["ids"]):
        metadata[i]["id"] = doc_id

    # 5. Create Nomic Atlas Map
    print(f"Creating Nomic Atlas project '{project_name}'...")
    try:
        project = atlas.map_data(
            data=metadata,
            embeddings=embeddings,
            identifier=project_name,
            id_field="id",
            topic_model=NomicTopicOptions(
                build_topic_model=True,
                topic_label_field="artist_name",
            ),
        )
        print("\nSUCCESS: Interactive Nomic map created!")
        print(f"View your map at: {project.maps[0].map_link}")
    except ValueError as e:
        if "token" in str(e).lower():
            print("\nERROR: Nomic API token issue.")
        else:
            print(f"\nERROR: Failed to create Nomic map: {e}")


if __name__ == "__main__":
    main()
