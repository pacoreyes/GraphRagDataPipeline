import pytest
from data_pipeline.utils.lastfm_helpers import parse_lastfm_artist_response, LastFmArtistInfo

def test_parse_lastfm_artist_response_valid():
    """Verify parsing of a standard Last.fm response."""
    response = {
        "artist": {
            "tags": {
                "tag": [{"name": "Electronic"}, {"name": "Synthpop"}]
            },
            "similar": {
                "artist": [{"name": "Kraftwerk"}]
            }
        }
    }
    info = parse_lastfm_artist_response(response)
    assert info.tags == ["Electronic", "Synthpop"]
    assert info.similar_artists == ["Kraftwerk"]

def test_parse_lastfm_artist_response_single_item():
    """Verify parsing when API returns a dict instead of a list for single items."""
    response = {
        "artist": {
            "tags": {"tag": {"name": "Techno"}},
            "similar": {"artist": {"name": "Orbital"}}
        }
    }
    info = parse_lastfm_artist_response(response)
    assert info.tags == ["Techno"]
    assert info.similar_artists == ["Orbital"]

def test_parse_lastfm_artist_response_empty():
    """Verify handling of empty or invalid responses."""
    assert parse_lastfm_artist_response(None).tags == []
    assert parse_lastfm_artist_response({}).tags == []
