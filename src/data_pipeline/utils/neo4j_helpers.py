# -----------------------------------------------------------
# Neo4j Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Any, LiteralString, cast

from dagster import AssetExecutionContext
from neo4j import Driver


def execute_cypher(
    driver: Driver,
    query: str,
    params: dict[str, Any] | None = None,
    database: str = "neo4j"
) -> None:
    """
    Executes a Cypher query with optional parameters using the Neo4j driver.
    """
    try:
        with driver.session(database=database) as session:
            # We cast to LiteralString because schema-altering queries cannot be parameterized,
            # and the driver uses LiteralString as a safety hint.
            session.run(cast(LiteralString, query), params or {}).consume()
    except Exception as e:
        raise e


def clear_database(driver: Driver, context: AssetExecutionContext) -> None:
    """
    Clears all nodes, relationships, and indexes from the database.
    """
    context.log.info("Starting database cleanup...")

    try:
        # 1. Delete all nodes and relationships
        # Using simple DETACH DELETE for smaller datasets. 
        # For huge datasets, batched deletion (CALL { ... } IN TRANSACTIONS) is preferred.
        # noinspection SqlNoDataSourceInspection
        execute_cypher(driver, "MATCH (n) DETACH DELETE n;")
        context.log.info("Deleted all nodes and relationships.")

        # 2. Drop all indexes
        with driver.session() as session:
            # noinspection SqlNoDataSourceInspection
            indexes = session.run("SHOW INDEXES").data()
            for idx in indexes:
                # Filter system indexes or those we shouldn't touch if necessary
                name = idx.get("name")
                type_ = idx.get("type", "").lower()
                
                # We only drop explicitly created indexes (RANGE, POINT, TEXT)
                if name and type_ in ["range", "point", "text", "btree"]: 
                    try:
                        # noinspection SqlNoDataSourceInspection
                        session.run(cast(LiteralString, f"DROP INDEX {name}"))
                        context.log.info(f"Dropped index: {name}")
                    except Exception as e:
                        context.log.warning(f"Failed to drop index {name}: {e}")

            # Drop constraints as well if they exist
            # noinspection SqlNoDataSourceInspection
            constraints = session.run("SHOW CONSTRAINTS").data()
            for const in constraints:
                name = const.get("name")
                if name:
                    try:
                        # noinspection SqlNoDataSourceInspection
                        session.run(cast(LiteralString, f"DROP CONSTRAINT {name}"))
                        context.log.info(f"Dropped constraint: {name}")
                    except Exception as e:
                        context.log.warning(f"Failed to drop constraint {name}: {e}")

    except Exception as e:
        context.log.error(f"Error during database cleanup: {e}")
        raise e
