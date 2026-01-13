
import pytest
from unittest.mock import MagicMock
from data_pipeline.utils.wikidata_helpers import (
    extract_wikidata_label, 
    extract_wikidata_aliases
)

def test_extract_wikidata_label_english_exists():
    data = {
        "labels": {
            "en": {"language": "en", "value": "English Label"},
            "de": {"language": "de", "value": "German Label"}
        }
    }
    assert extract_wikidata_label(data, lang="en", languages=["en", "de"]) == "English Label"

def test_extract_wikidata_label_fallback():
    # Case: English missing, German exists
    data = {
        "labels": {
            "de": {"language": "de", "value": "German Label"},
            "es": {"language": "es", "value": "Spanish Label"}
        }
    }
    assert extract_wikidata_label(data, lang="en", languages=["de", "es"]) == "German Label"

def test_extract_wikidata_label_none_found():
    data = {
        "labels": {
            "xx": {"language": "xx", "value": "Unknown Language Label"}
        }
    }
    assert extract_wikidata_label(data, lang="en", languages=["en"]) is None

def test_extract_wikidata_aliases_english_exists():
    data = {
        "aliases": {
            "en": [{"language": "en", "value": "Alias 1"}, {"language": "en", "value": "Alias 2"}]
        }
    }
    assert extract_wikidata_aliases(data, lang="en", languages=["en"]) == ["Alias 1", "Alias 2"]

def test_extract_wikidata_aliases_fallback():
    data = {
        "aliases": {
            "de": [{"language": "de", "value": "German Alias"}]
        }
    }
    assert extract_wikidata_aliases(data, lang="en", languages=["de"]) == ["German Alias"]
