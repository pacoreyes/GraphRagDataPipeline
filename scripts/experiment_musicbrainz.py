
import musicbrainzngs
import json
from pathlib import Path

# Configure MusicBrainz
musicbrainzngs.set_useragent("MusicDiscographyExperiment", "1.0", "test@nodesAI.de")

def get_depeche_mode_discography():
    print("Searching for artist 'Depeche Mode'...")
    search_results = musicbrainzngs.search_artists(artist="Depeche Mode")
    
    if not search_results['artist-list']:
        print("Artist not found.")
        return

    artist = search_results['artist-list'][0]
    artist_id = artist['id']
    print(f"Found artist: {artist['name']} (ID: {artist_id})")

    print("Fetching release groups (discography)...")
    # Fetch release groups (albums, singles, etc.)
    # We use browse_release_groups because an artist can have many releases
    limit = 100
    offset = 0
    all_release_groups = []
    
    while True:
        result = musicbrainzngs.browse_release_groups(artist=artist_id, limit=limit, offset=offset)
        release_groups = result['release-group-list']
        all_release_groups.extend(release_groups)
        
        if len(release_groups) < limit:
            break
        offset += limit

    print(f"Total release groups found (raw): {len(all_release_groups)}")

    # Filter: Primary type Album or Single, AND NO Secondary types
    filtered_groups = []
    for rg in all_release_groups:
        primary = rg.get('primary-type')
        secondary = rg.get('secondary-type-list', [])
        
        if primary in ["Album", "Single"] and not secondary:
            filtered_groups.append(rg)

    print(f"Total release groups (filtered): {len(filtered_groups)}")

    output_path = Path("scripts/depeche_mode_filtered_discography.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(filtered_groups, f, indent=2, ensure_ascii=False)
    
    print(f"Filtered discography saved to {output_path}")

if __name__ == "__main__":
    try:
        get_depeche_mode_discography()
    except Exception as e:
        print(f"An error occurred: {e}")
