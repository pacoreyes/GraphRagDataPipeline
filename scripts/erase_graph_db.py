# -----------------------------------------------------------
# Erase Graph DB Script
# Standalone Script for Erasing Neo4j Database
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
Standalone script to completely erase the Neo4j database content.

This script provides a convenient way to clear all nodes, relationships, and 
indexes from the Neo4j database. It reuses the core logic from the 
ETL application to ensure consistency.

Usage:
    python -m scripts.erase_graph_db
"""

import os
import sys
from typing import Any

from dagster import build_asset_context
from neo4j import GraphDatabase

from data_pipeline.utils.neo4j_helpers import (
    clear_database,
)


def main() -> None:
    """
    Main execution function for the database erasure script.
    """
    print("--- Neo4j Database Erasure Tool ---")
    
    # 1. Initialize Client and Check Connectivity
    uri = os.environ.get("NEO4J_URI")
    username = os.environ.get("NEO4J_USERNAME")
    password = os.environ.get("NEO4J_PASSWORD")

    if not all([uri, username, password]):
        print("Error: Missing Neo4j environment variables (NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD).")
        sys.exit(1)

    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        # Check connectivity
        driver.verify_connectivity()
        
        # Retrieve URI from driver for display (or settings)
        db_uri = driver.get_server_info().address
    except Exception as e:
        print(f"Error: Could not connect to Neo4j.")
        print(f"Details: {e}")
        sys.exit(1)

    print(f"Connected to Neo4j at {db_uri}.")
    
    # 2. Confirmation Prompt
    confirm = input(
        "\nWARNING: This will delete ALL data and indexes in the database.\n"
        "Are you sure you want to proceed? (yes/no): "
    )
    
    if confirm.lower() != "yes":
        print("Operation cancelled.")
        driver.close()
        return

    # 4. Create Context and Execute Cleanup
    # We use build_asset_context() to satisfy the requirement for an 
    # AssetExecutionContext used for logging within clear_database.
    context: Any = build_asset_context()

    print("\nStarting database erasure...")
    try:
        clear_database(driver, context)
        print("\nSUCCESS: Database successfully erased.")
    except Exception as e:
        print(f"\nERROR: Failed to erase database: {e}")
        sys.exit(1)
    finally:
        driver.close()


if __name__ == "__main__":
    main()


""" DO NOT DELETE
uv run --env-file .env scripts/erase_graph_db.py

MATCH path = (art:Artist {name: "Arcade Fire"})-[:SIMILAR_TO]->(similar:Artist)
RETURN path;
"""