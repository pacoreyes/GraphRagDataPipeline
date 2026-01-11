# -----------------------------------------------------------
# Dagster Resources
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from contextlib import contextmanager, asynccontextmanager
from typing import Any, AsyncGenerator, Generator

import httpx
from dagster import (
    ConfigurableResource,
    Definitions,
    EnvVar,
)
from neo4j import GraphDatabase, Driver

from data_pipeline.settings import settings
from .io_managers import PolarsJSONLIOManager


class LastFmResource(ConfigurableResource):
    """
    Configuration for Last.fm API.
    """
    api_key: str


class NomicResource(ConfigurableResource):
    """
    Configuration for Nomic API.
    """
    api_key: str


class Neo4jResource(ConfigurableResource):
    """
    Resource for interacting with the Neo4j graph database.
    """
    uri: str
    username: str
    password: str

    @contextmanager
    def get_driver(self, context) -> Generator[Driver, None, None]:
        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        try:
            yield driver
        finally:
            driver.close()


class WikidataResource(ConfigurableResource):
    """
    Resource for making HTTP requests to Wikidata (SPARQL).
    """
    user_agent: str
    timeout: int

    @asynccontextmanager
    async def get_client(self, context) -> AsyncGenerator[httpx.AsyncClient, None]:
        async with httpx.AsyncClient(
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout
        ) as client:
            yield client


# Base resources
resource_defs: dict[str, Any] = {
    "lastfm": LastFmResource(
        api_key=EnvVar("LASTFM_API_KEY")
    ),
    "nomic": NomicResource(
        api_key=EnvVar("NOMIC_API_KEY")
    ),
    "neo4j": Neo4jResource(
        uri=EnvVar("NEO4J_URI"),
        username=EnvVar("NEO4J_USERNAME"),
        password=EnvVar("NEO4J_PASSWORD"),
    ),
    "wikidata": WikidataResource(
        user_agent=settings.USER_AGENT,
        timeout=settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT
    ),
    "io_manager": PolarsJSONLIOManager(base_dir=str(settings.DATASETS_DIRPATH))
}

# Export as Definitions for automatic loading
defs = Definitions(
    resources=resource_defs,
)
