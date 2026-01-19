import pytest
import torch
from unittest.mock import MagicMock, patch
from data_pipeline.utils.chroma_helpers import get_device, generate_doc_id

def test_get_device():
    """Verify that get_device returns a torch.device."""
    device = get_device()
    assert isinstance(device, torch.device)

def test_generate_doc_id():
    """Verify that doc IDs are consistent and truncated to 32 chars."""
    text = "Hello World"
    row_hash = "row_1"
    doc_id = generate_doc_id(text, row_hash)
    
    assert len(doc_id) == 32
    # Consistency
    assert generate_doc_id(text, row_hash) == doc_id
    # Uniqueness
    assert generate_doc_id("different", row_hash) != doc_id
