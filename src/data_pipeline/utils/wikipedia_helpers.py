# -----------------------------------------------------------
# Wikipedia API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# ----------------------------------------------------------- 

import urllib.parse
from pathlib import Path
from typing import Optional

from dagster import AssetExecutionContext

from data_pipeline.utils.network_helpers import (
    make_async_request_with_retries,
    AsyncClient,
    HTTPError,
)
from data_pipeline.utils.io_helpers import async_read_text_file, async_write_text_file


async def async_fetch_wikipedia_article(
    context: AssetExecutionContext,
    title: str,
    qid: str,
    api_url: str,
    cache_dir: Path,
    headers: Optional[dict[str, str]] = None,
    client: Optional[AsyncClient] = None,
) -> Optional[str]:
    """
    Fetches the raw plain text of a Wikipedia article by its title, with caching.
    The QID is required to ensure consistent cache file naming (e.g., Q123.txt).
    """
    clean_title = urllib.parse.unquote(title).replace("_", " ")
    
    # Use QID for cache filename
    cache_file = cache_dir / f"{qid}.txt"
    
    # 1. Check Cache
    cached_content = await async_read_text_file(cache_file)
    if cached_content:
        return cached_content

    # 2. Fetch from API
    params = {
        "action": "query",
        "format": "json",
        "titles": clean_title,
        "prop": "extracts",
        "explaintext": "True",
        "redirects": 1,
    }

    try:
        response = await make_async_request_with_retries(
            context=context,
            url=api_url,
            method="GET",
            params=params,
            headers=headers,
            client=client,
        )
        
        data = response.json()
        pages = (data.get("query") or {}).get("pages") or {}
        
        for page_id, page_data in pages.items():
            if page_id == "-1":
                return None
            
            extract = page_data.get("extract")
            if extract:
                # 3. Save to Cache
                await async_write_text_file(cache_file, extract)
                return extract
            
    except (HTTPError, ValueError):
        return None

    return None
