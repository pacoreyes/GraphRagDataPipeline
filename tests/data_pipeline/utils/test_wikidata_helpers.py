
import pytest
from unittest.mock import MagicMock
from data_pipeline.utils.wikidata_helpers import (
    extract_wikidata_label, 
    extract_wikidata_aliases, 
    FALLBACK_LANGUAGES
)

def test_extract_wikidata_label_english_exists():
    data = {
        "labels": {
            "en": {"language": "en", "value": "English Label"},
            "de": {"language": "de", "value": "German Label"}
        }
    }
    assert extract_wikidata_label(data, lang="en") == "English Label"

def test_extract_wikidata_label_fallback():
    # Case: English missing, German exists (and 'de' is in FALLBACK_LANGUAGES)
    data = {
        "labels": {
            "de": {"language": "de", "value": "German Label"},
            "es": {"language": "es", "value": "Spanish Label"}
        }
    }
    # Should pick the first one from FALLBACK_LANGUAGES that exists
    # Assuming "de" comes before "es" in FALLBACK_LANGUAGES
    assert extract_wikidata_label(data, lang="en") == "German Label"

def test_extract_wikidata_label_none_found():
    data = {
        "labels": {
            "xx": {"language": "xx", "value": "Unknown Language Label"}
        }
    }
    assert extract_wikidata_label(data, lang="en") is None

def test_extract_wikidata_aliases_english_exists():
    data = {
        "aliases": {
            "en": [{"language": "en", "value": "Alias 1"}, {"language": "en", "value": "Alias 2"}]
        }
    }
    assert extract_wikidata_aliases(data, lang="en") == ["Alias 1", "Alias 2"]

def test_extract_wikidata_aliases_fallback():
    data = {
        "aliases": {
            "de": [{"language": "de", "value": "German Alias"}]
        }
    }
    assert extract_wikidata_aliases(data, lang="en") == ["German Alias"]

def test_fallback_languages_sanity():
    assert "en" in FALLBACK_LANGUAGES
    assert "de" in FALLBACK_LANGUAGES
