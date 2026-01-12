# -----------------------------------------------------------
# MusicBrainz API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import time
import musicbrainzngs
from typing import Any, List, Dict, Optional
from data_pipeline.settings import settings

def setup_musicbrainz():
    """Configures the MusicBrainz client with the project User-Agent."""
    musicbrainzngs.set_useragent(
        settings.APP_NAME, 
        settings.APP_VERSION, 
        settings.CONTACT_EMAIL
    )
    # Global rate limit compliance for the library if needed, 
    # though we handle it via our own delays usually.
    musicbrainzngs.set_rate_limit(limit_or_interval=1.0, new_requests=1)

def get_artist_release_groups_filtered(artist_mbid: str) -> List[Dict[str, Any]]:
    """
    Fetches all release groups for an artist, filtered by:
    - Primary Type: 'Album' or 'Single'
    - Secondary Type: None/Empty
    
    Handles pagination automatically.
    """
    setup_musicbrainz()
    
    limit = 100
    offset = 0
    all_release_groups = []
    
    try:
        while True:
            # Synch call
            result = musicbrainzngs.browse_release_groups(
                artist=artist_mbid, 
                limit=limit, 
                offset=offset
            )
            batch = result.get('release-group-list', [])
            all_release_groups.extend(batch)
            
            if len(batch) < limit:
                break
                
            offset += limit
            # Polite sleep between pages
            time.sleep(settings.MUSICBRAINZ_RATE_LIMIT_DELAY)
            
    except musicbrainzngs.MusicBrainzError as e:
        # Log or re-raise? For helpers, usually best to let caller handle or return what we have.
        # We'll return what we have found so far if it fails mid-way.
        print(f"MusicBrainz API Error for {artist_mbid}: {e}")
        return all_release_groups

    # Filter
    filtered_groups = []
    for rg in all_release_groups:
        primary = rg.get('primary-type')
        secondary = rg.get('secondary-type-list', [])
        
        if primary in ["Album", "Single"] and not secondary:
            filtered_groups.append(rg)
            
    return filtered_groups

def get_release_group_details(release_group_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetches detailed information for a specific release group.
    """
    setup_musicbrainz()
    try:
        # Fetch details including tags, genres, etc.
        # Note: 'releases' includes the list of concrete releases (CDs, etc)
        return musicbrainzngs.get_release_group_by_id(
            release_group_id, 
            includes=["tags", "genres", "url-rels", "annotation"]
        )
    except musicbrainzngs.MusicBrainzError:
        return None
