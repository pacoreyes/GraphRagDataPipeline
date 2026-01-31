# Work Plan: Community Layer for GraphRAG

**Created**: January 30, 2026
**Status**: In Progress

---

## Overview

Add community detection and summarization to the existing data pipeline using **standalone experimental scripts** in the `scripts/` folder. No modifications to existing app code.

---

## Design Decisions

1. **Graph edges for community detection**: SIMILAR_TO + PLAYS_GENRE (bipartite graph of Artists and Genres)
2. **Genres as first-class nodes**: Genres get community assignments too (richer for Global Search)
3. **ChromaDB storage**: Same collection (`music_rag_collection`) with `entity_type="community"`
4. **LLM Model**: Qwen2.5-14B-Instruct-4bit via MLX (quality over speed, fits overnight)

---

## Phase 1: Setup & Dependencies

- [x] **Task 1.1**: Install required packages
  ```bash
  uv add igraph leidenalg mlx-lm
  ```

- [ ] **Task 1.2**: Download the LLM model
  ```bash
  mlx_lm.download --repo mlx-community/Qwen2.5-14B-Instruct-4bit
  ```

---

## Phase 2: Community Detection Script

**Script**: `scripts/detect_communities.py`

**Purpose**: Extract graph from Neo4j, run Leiden algorithm, output community assignments.

**Input**:
- Neo4j database (reads Artist nodes, Genre nodes, SIMILAR_TO and PLAYS_GENRE relationships)
- Environment variables: `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`

**Process**:
1. Connect to Neo4j and export:
   - All Artist nodes
   - All Genre nodes
   - SIMILAR_TO edges (Artist → Artist)
   - PLAYS_GENRE edges (Artist → Genre)
2. Build igraph Graph object (bipartite: Artists + Genres as nodes)
3. Run Leiden algorithm at 3 resolution levels:
   - Level 0: Fine-grained (~300-500 communities)
   - Level 1: Medium (~50-100 communities)
   - Level 2: Coarse (~10-20 macro-themes)
4. Output community assignments

**Output**: `data_volume/datasets/community_assignments.parquet`
```
| entity_id | entity_name  | entity_type | community_L0 | community_L1 | community_L2 |
|-----------|--------------|-------------|--------------|--------------|--------------|
| Q1234     | Daft Punk    | artist      | 42           | 7            | 2            |
| Q56789    | French House | genre       | 42           | 7            | 2            |
```

**Estimated Runtime**: ~5-10 minutes

---

## Phase 3: Community Metadata Aggregation Script

**Script**: `scripts/aggregate_community_metadata.py`

**Purpose**: For each community, aggregate member metadata to prepare LLM prompts.

**Input**:
- `data_volume/datasets/community_assignments.parquet` (from Phase 2)
- `data_volume/datasets/artists.parquet` (existing asset)

**Process**:
1. Join community assignments with artist metadata
2. For each community at each level, compute:
   - Member count
   - Top 10 tags (by frequency)
   - Top 5 genres (by frequency)
   - Top 3 countries (by frequency)
   - Representative artists (top 5 by connectivity)
   - Decade distribution (when artists were active)

**Output**: `data_volume/datasets/community_metadata.parquet`
```
| community_id | level | member_count | top_tags | top_genres | top_countries | representative_artists | decade_distribution |
```

**Estimated Runtime**: ~2-3 minutes

---

## Phase 4: Community Summary Generation Script

**Script**: `scripts/generate_community_summaries.py`

**Purpose**: Use Qwen2.5-14B via MLX to generate natural language summaries.

**Input**:
- `data_volume/datasets/community_metadata.parquet` (from Phase 3)

**Process**:
1. Load Qwen2.5-14B-Instruct-4bit model via mlx-lm
2. For each community, construct prompt:
   ```
   You are a music historian. Summarize this community of electronic music artists:
   - Member count: {count}
   - Top genres: {genres}
   - Top tags: {tags}
   - Countries: {countries}
   - Representative artists: {artists}
   - Active decades: {decades}

   Write a 2-3 sentence summary describing the musical identity of this community.
   ```
3. Generate summary (max 150 tokens per community)
4. Save incrementally (resume-safe)

**Output**: `data_volume/datasets/community_summaries.parquet`
```
| community_id | level | summary | member_count | top_genres | top_tags |
```

**Estimated Runtime**: ~8-12 hours (overnight) for ~600-700 communities across 3 levels

---

## Phase 5: ChromaDB Ingestion Script

**Script**: `scripts/ingest_community_summaries.py`

**Purpose**: Embed community summaries into ChromaDB for semantic search.

**Input**:
- `data_volume/datasets/community_summaries.parquet` (from Phase 4)

**Process**:
1. Connect to existing ChromaDB collection (`music_rag_collection`)
2. For each community summary:
   - Generate embedding using Nomic (same model as existing docs)
   - Create document with metadata:
     ```python
     {
         "id": f"community_L{level}_{community_id}",
         "document": summary,
         "metadata": {
             "entity_type": "community",
             "level": level,
             "member_count": count,
             "top_genres": genres,
             "top_tags": tags,
             "member_ids": [list of artist QIDs]  # For drill-down
         }
     }
     ```
3. Upsert into ChromaDB

**Output**: Community summaries added to `music_rag_collection` in ChromaDB

**Estimated Runtime**: ~10-15 minutes

---

## Phase 6: Validation Script

**Script**: `scripts/validate_communities.py`

**Purpose**: Verify the community layer is correctly built.

**Checks**:
1. All artists have community assignments at all 3 levels
2. Community summaries exist for all communities
3. ChromaDB contains community documents
4. Sample queries return expected results:
   - "German techno from the 90s" → relevant communities
   - "French house pioneers" → relevant communities

**Output**: Validation report printed to console

---

## Execution Order

```
1. Install dependencies (uv add igraph leidenalg mlx-lm)        ✅ Done
2. Download model (mlx_lm.download)                             ⏳ In progress
3. Run: python scripts/detect_communities.py                    (~10 min)
4. Run: python scripts/aggregate_community_metadata.py          (~3 min)
5. Run: python scripts/generate_community_summaries.py          (~8-12 hours, overnight)
6. Run: python scripts/ingest_community_summaries.py            (~15 min)
7. Run: python scripts/validate_communities.py                  (~2 min)
```

**Total Active Time**: ~30 minutes setup + supervision
**Total Passive Time**: ~8-12 hours (overnight LLM generation)

---

## Files to Create

| Script | Purpose | Dependencies |
|--------|---------|--------------|
| `scripts/detect_communities.py` | Leiden community detection | igraph, leidenalg, neo4j |
| `scripts/aggregate_community_metadata.py` | Metadata aggregation | polars |
| `scripts/generate_community_summaries.py` | LLM summarization | mlx-lm |
| `scripts/ingest_community_summaries.py` | ChromaDB ingestion | chromadb, nomic |
| `scripts/validate_communities.py` | Validation checks | polars, chromadb, neo4j |

---

## No Modifications To

- `src/data_pipeline/` (no changes to assets, models, settings)
- Existing Parquet files (read-only)
- Existing ChromaDB collection structure (additive only)
- Existing Neo4j schema (read-only)

---

## Future Integration (After Validation)

Once the experimental scripts are validated, the community layer can optionally be integrated into the main Dagster pipeline as proper assets. But that's a separate work item.
