# -----------------------------------------------------------
# Tests for Neo4j Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from unittest.mock import MagicMock

import pytest
from neo4j.exceptions import ServiceUnavailable, SessionExpired

from data_pipeline.utils.neo4j_helpers import (
    _execute_with_retry,
    build_igraph,
    clear_database,
    execute_cypher,
    get_community_stats,
    run_leiden_multilevel,
)


@pytest.fixture
def mock_driver():
    """Creates a mock Neo4j driver."""
    return MagicMock()


@pytest.fixture
def mock_context():
    """Creates a mock Dagster AssetExecutionContext."""
    context = MagicMock()
    context.log = MagicMock()
    return context


class TestExecuteCypher:
    """Tests for execute_cypher function."""

    def test_execute_cypher_transactional_success(self, mock_driver):
        """Test execute_cypher with transactional=True uses execute_write."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        execute_cypher(mock_driver, "CREATE (n:Test)", {"name": "test"})

        mock_driver.session.assert_called_once_with(database="neo4j")
        mock_session.execute_write.assert_called_once()

    def test_execute_cypher_non_transactional_success(self, mock_driver):
        """Test execute_cypher with transactional=False uses session.run."""
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        execute_cypher(
            mock_driver,
            "CREATE INDEX test_idx FOR (n:Test) ON (n.name)",
            transactional=False
        )

        mock_driver.session.assert_called_once_with(database="neo4j")
        mock_session.run.assert_called_once()
        mock_result.consume.assert_called_once()

    def test_execute_cypher_raises_on_error(self, mock_driver):
        """Test that exceptions are propagated correctly."""
        mock_session = MagicMock()
        mock_session.execute_write.side_effect = Exception("Connection failed")
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(Exception, match="Connection failed"):
            execute_cypher(mock_driver, "CREATE (n:Test)")


class TestExecuteWithRetry:
    """Tests for _execute_with_retry function."""

    def test_execute_with_retry_success_first_attempt(self, mock_driver):
        """Test successful execution on first attempt."""
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.single.return_value = {"count": 10}
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        result = _execute_with_retry(mock_driver, "MATCH (n) RETURN count(n) AS count")

        assert result == {"count": 10}
        assert mock_driver.session.call_count == 1

    def test_execute_with_retry_recovers_from_transient_failure(self, mock_driver):
        """Test retry logic recovers from SessionExpired."""
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.single.return_value = {"deleted": 100}

        # First call fails, second succeeds
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise SessionExpired("Connection lost")
            return mock_result

        mock_session.run.side_effect = side_effect
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        result = _execute_with_retry(
            mock_driver,
            "MATCH (n) DELETE n RETURN count(*) AS deleted",
            max_retries=3,
            base_delay=0.01  # Fast retry for tests
        )

        assert result == {"deleted": 100}
        assert call_count[0] == 2

    def test_execute_with_retry_exhausts_retries(self, mock_driver):
        """Test that exception is raised after max retries."""
        mock_session = MagicMock()
        mock_session.run.side_effect = ServiceUnavailable("Service unavailable")
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(ServiceUnavailable, match="Service unavailable"):
            _execute_with_retry(
                mock_driver,
                "MATCH (n) RETURN n",
                max_retries=2,
                base_delay=0.01
            )

        # Should have tried 3 times (initial + 2 retries)
        assert mock_session.run.call_count == 3


class TestClearDatabase:
    """Tests for clear_database function."""

    def test_clear_database_deletes_relationships_then_nodes(self, mock_driver, mock_context):
        """Test that clear_database deletes relationships first, then nodes."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate: relationships deleted, then nodes deleted
        mock_results = [
            MagicMock(single=MagicMock(return_value={"deleted": 50})),   # rels batch 1
            MagicMock(single=MagicMock(return_value={"deleted": 0})),    # rels done
            MagicMock(single=MagicMock(return_value={"deleted": 100})),  # nodes batch 1
            MagicMock(single=MagicMock(return_value={"deleted": 0})),    # nodes done
            MagicMock(data=MagicMock(return_value=[])),  # SHOW INDEXES
            MagicMock(data=MagicMock(return_value=[])),  # SHOW CONSTRAINTS
        ]
        mock_session.run.side_effect = mock_results

        clear_database(mock_driver, mock_context, batch_size=100)

        # Verify logging
        mock_context.log.info.assert_any_call("Starting database cleanup...")
        mock_context.log.info.assert_any_call("Deleted 50 relationships.")
        mock_context.log.info.assert_any_call("Deleted 100 nodes.")

    def test_clear_database_handles_empty_database(self, mock_driver, mock_context):
        """Test clear_database handles an already empty database."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # Database is already empty
        mock_results = [
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No rels
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No nodes
            MagicMock(data=MagicMock(return_value=[])),  # SHOW INDEXES
            MagicMock(data=MagicMock(return_value=[])),  # SHOW CONSTRAINTS
        ]
        mock_session.run.side_effect = mock_results

        clear_database(mock_driver, mock_context)

        mock_context.log.info.assert_any_call("Deleted 0 nodes.")

    def test_clear_database_drops_indexes(self, mock_driver, mock_context):
        """Test that clear_database drops existing indexes."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_indexes_result = MagicMock()
        mock_indexes_result.data.return_value = [
            {"name": "artist_idx", "type": "RANGE"},
            {"name": "genre_idx", "type": "BTREE"},
        ]

        mock_constraints_result = MagicMock()
        mock_constraints_result.data.return_value = []

        mock_results = [
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No rels
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No nodes
            mock_indexes_result,  # SHOW INDEXES
            MagicMock(),  # DROP INDEX artist_idx
            MagicMock(),  # DROP INDEX genre_idx
            mock_constraints_result,  # SHOW CONSTRAINTS
        ]
        mock_session.run.side_effect = mock_results

        clear_database(mock_driver, mock_context)

        # Verify indexes were dropped
        mock_context.log.info.assert_any_call("Dropped index: artist_idx")
        mock_context.log.info.assert_any_call("Dropped index: genre_idx")

    def test_clear_database_drops_constraints(self, mock_driver, mock_context):
        """Test that clear_database drops existing constraints."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_indexes_result = MagicMock()
        mock_indexes_result.data.return_value = []

        mock_constraints_result = MagicMock()
        mock_constraints_result.data.return_value = [
            {"name": "artist_unique"},
        ]

        mock_results = [
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No rels
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No nodes
            mock_indexes_result,  # SHOW INDEXES
            mock_constraints_result,  # SHOW CONSTRAINTS
            MagicMock(),  # DROP CONSTRAINT
        ]
        mock_session.run.side_effect = mock_results

        clear_database(mock_driver, mock_context)

        mock_context.log.info.assert_any_call("Dropped constraint: artist_unique")

    def test_clear_database_raises_on_persistent_error(self, mock_driver, mock_context):
        """Test that errors are raised after retries are exhausted."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.run.side_effect = ServiceUnavailable("Persistent failure")

        with pytest.raises(ServiceUnavailable, match="Persistent failure"):
            clear_database(mock_driver, mock_context)

        mock_context.log.error.assert_called_once()


# =============================================================================
# Graph Construction and Community Detection Tests
# =============================================================================

class TestBuildIgraph:
    """Tests for build_igraph function."""

    def test_build_igraph_basic(self):
        """Test building a simple graph."""
        nodes = [
            {"id": "A", "name": "Node A"},
            {"id": "B", "name": "Node B"},
            {"id": "C", "name": "Node C"},
        ]
        edges = [("A", "B"), ("B", "C")]

        graph, id_to_idx = build_igraph(nodes, edges)

        assert graph.vcount() == 3
        assert graph.ecount() == 2
        assert id_to_idx == {"A": 0, "B": 1, "C": 2}

    def test_build_igraph_with_node_attrs(self):
        """Test building graph with additional node attributes."""
        nodes = [
            {"id": "A", "name": "Node A", "type": "artist"},
            {"id": "B", "name": "Node B", "type": "genre"},
        ]
        edges = [("A", "B")]

        graph, _ = build_igraph(nodes, edges, node_attrs=["name", "type"])

        assert graph.vs[0]["name"] == "Node A"
        assert graph.vs[0]["type"] == "artist"
        assert graph.vs[1]["type"] == "genre"

    def test_build_igraph_filters_invalid_edges(self):
        """Test that edges with non-existent nodes are filtered."""
        nodes = [
            {"id": "A", "name": "Node A"},
            {"id": "B", "name": "Node B"},
        ]
        edges = [("A", "B"), ("A", "C"), ("D", "B")]  # C and D don't exist

        graph, _ = build_igraph(nodes, edges)

        assert graph.vcount() == 2
        assert graph.ecount() == 1  # Only A-B edge

    def test_build_igraph_empty_graph(self):
        """Test building an empty graph."""
        graph, id_to_idx = build_igraph([], [])

        assert graph.vcount() == 0
        assert graph.ecount() == 0
        assert id_to_idx == {}

    def test_build_igraph_custom_id_key(self):
        """Test building graph with custom ID key."""
        nodes = [
            {"wikidata_id": "Q1", "name": "Entity 1"},
            {"wikidata_id": "Q2", "name": "Entity 2"},
        ]
        edges = [("Q1", "Q2")]

        graph, id_to_idx = build_igraph(nodes, edges, node_id_key="wikidata_id")

        assert id_to_idx == {"Q1": 0, "Q2": 1}
        assert graph.vs[0]["wikidata_id"] == "Q1"


class TestRunLeidenMultilevel:
    """Tests for run_leiden_multilevel function."""

    def test_run_leiden_single_resolution(self):
        """Test Leiden with a single resolution level."""
        nodes = [{"id": str(i)} for i in range(10)]
        # Create a connected graph
        edges = [(str(i), str(i + 1)) for i in range(9)]

        graph, _ = build_igraph(nodes, edges)
        memberships = run_leiden_multilevel(graph, [1.0])

        assert len(memberships) == 1
        assert len(memberships[0]) == 10  # One assignment per node

    def test_run_leiden_multiple_resolutions(self):
        """Test Leiden with multiple resolution levels."""
        nodes = [{"id": str(i)} for i in range(20)]
        # Create two clusters
        edges = (
            [(str(i), str(j)) for i in range(10) for j in range(i + 1, 10)]  # Cluster 1
            + [(str(i), str(j)) for i in range(10, 20) for j in range(i + 1, 20)]  # Cluster 2
            + [("5", "15")]  # Bridge
        )

        graph, _ = build_igraph(nodes, edges)
        memberships = run_leiden_multilevel(graph, [2.0, 0.5, 0.1])

        assert len(memberships) == 3
        # Higher resolution should have more communities
        assert len(set(memberships[0])) >= len(set(memberships[2]))

    def test_run_leiden_deterministic_with_seed(self):
        """Test that Leiden produces deterministic results with same seed."""
        nodes = [{"id": str(i)} for i in range(10)]
        edges = [(str(i), str((i + 1) % 10)) for i in range(10)]

        graph, _ = build_igraph(nodes, edges)

        memberships1 = run_leiden_multilevel(graph, [1.0], seed=42)
        memberships2 = run_leiden_multilevel(graph, [1.0], seed=42)

        assert memberships1 == memberships2


class TestGetCommunityStats:
    """Tests for get_community_stats function."""

    def test_get_community_stats_basic(self):
        """Test computing stats for a simple partition."""
        membership = [0, 0, 0, 1, 1, 2]

        stats = get_community_stats(membership)

        assert stats["num_communities"] == 3
        assert stats["largest"] == 3
        assert stats["smallest"] == 1
        assert stats["mean_size"] == 2.0
        assert stats["sizes"] == [3, 2, 1]

    def test_get_community_stats_single_community(self):
        """Test stats with all nodes in one community."""
        membership = [0, 0, 0, 0, 0]

        stats = get_community_stats(membership)

        assert stats["num_communities"] == 1
        assert stats["largest"] == 5
        assert stats["smallest"] == 5
        assert stats["mean_size"] == 5.0

    def test_get_community_stats_empty(self):
        """Test stats with empty membership."""
        stats = get_community_stats([])

        assert stats["num_communities"] == 0
        assert stats["largest"] == 0
        assert stats["smallest"] == 0
        assert stats["mean_size"] == 0
