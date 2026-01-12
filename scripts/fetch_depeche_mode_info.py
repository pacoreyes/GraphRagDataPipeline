
import musicbrainzngs
import json
from pathlib import Path

# Configure MusicBrainz
musicbrainzngs.set_useragent("ArtistInfoExperiment", "1.0", "test@nodesAI.de")

def fetch_artist_info():
    artist_name = "Depeche Mode"
    print(f"Searching for artist '{artist_name}'...")
    
    # Search for the artist
    search_results = musicbrainzngs.search_artists(artist=artist_name)
    
    if not search_results['artist-list']:
        print("Artist not found.")
        return

    # Take the most relevant artist match
    artist = search_results['artist-list'][0]
    artist_id = artist['id']
    print(f"Found artist: {artist['name']} (ID: {artist_id})")

    # Fetch detailed artist info with extra data
    print("Fetching detailed artist information...")
    artist_details = musicbrainzngs.get_artist_by_id(
        artist_id, 
        includes=["aliases", "tags", "url-rels", "annotation"]
    )

    output_path = Path("scripts/depeche_mode_info.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(artist_details, f, indent=2, ensure_ascii=False)
    
    print(f"Artist information saved to {output_path}")

if __name__ == "__main__":
    try:
        fetch_artist_info()
    except Exception as e:
        print(f"An error occurred: {e}")
