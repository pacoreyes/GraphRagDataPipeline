import pytest
from data_pipeline.defs.partitions import decade_partitions, DECADES_TO_EXTRACT

def test_partitions_defined():
    """Verify that partitions match the expected decades."""
    partition_keys = decade_partitions.get_partition_keys()
    expected_keys = list(DECADES_TO_EXTRACT.keys())
    assert sorted(partition_keys) == sorted(expected_keys)

def test_decade_ranges():
    """Verify the year ranges for decades."""
    assert DECADES_TO_EXTRACT["1980s"] == (1980, 1989)
    assert DECADES_TO_EXTRACT["2020s"] == (2020, 2029)
