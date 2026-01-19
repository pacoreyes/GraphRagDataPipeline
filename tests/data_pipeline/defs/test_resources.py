import pytest
from unittest.mock import MagicMock
from dagster import build_init_resource_context
from data_pipeline.defs.resources import LastFmResource, MusicBrainzResource, Neo4jResource, resource_defs

def test_lastfm_resource_instantiation():
    """Check that LastFmResource can be initialized."""
    resource = LastFmResource(api_key="test_key")
    assert resource.api_key == "test_key"

def test_musicbrainz_resource_instantiation():
    """Check that MusicBrainzResource can be initialized."""
    resource = MusicBrainzResource(
        api_url="http://test",
        request_timeout=10,
        rate_limit_delay=1.0,
        cache_dir="/tmp"
    )
    assert resource.api_url == "http://test"

def test_resource_defs_contain_expected_keys():
    """Verify that all required resources are in the base resource_defs."""
    expected_keys = {"lastfm", "musicbrainz", "nomic", "chromadb", "neo4j", "wikidata", "wikipedia", "io_manager", "jsonl_io_manager"}
    assert expected_keys.issubset(set(resource_defs.keys()))
