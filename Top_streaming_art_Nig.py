import json
import requests
import os
import pandas as pd
from secret_file import spotify_user_id
from datetime import datetime, timedelta
from refresh_token import Refresh
from bson.objectid import ObjectId

class SaveSongs:
    def __init__(self):
        self.user_id = spotify_user_id
        self.spotify_token = ""
        self.tracks = ""

    def get_top_tracks_nigeria(self):
        # Use Spotify API endpoint to get the top tracks in Nigeria playlist
        endpoint = ""
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.spotify_token)
        }
        
        r = requests.get(endpoint, headers=headers, params = {"limit":50})
        if r.status_code not in range(200, 299):
            return {}
        return r.json()

    def call_refresh(self):
        print("Refreshing token")
        refreshCaller = Refresh()
        self.spotify_token = refreshCaller.refresh()

if __name__ == "__main__":
    a = SaveSongs()
    a.call_refresh()

    data = a.get_top_tracks_nigeria()

    # Extract relevant information from the data
    tracks = data.get('tracks', {}).get('items', [])
    tracks_info = []

    for track in tracks:
        track_name = track.get('track', {}).get('name', '')
        track_artists = ", ".join([artist.get('name', '') for artist in track.get('track', {}).get('artists', [])])
        track_duration_ms = track.get('track', {}).get('duration_ms', 0)
        track_popularity = track.get('track', {}).get('popularity', 0)
        track_release_date = track.get('track', {}).get('album', {}).get('release_date', '')
        track_album_name = track.get('track', {}).get('album', {}).get('name', '')

        tracks_info.append({
            'name': track_name,
            'artists': track_artists,
            'duration_ms': track_duration_ms,
            'popularity': track_popularity,
            'release_date': track_release_date,
            'album_name': track_album_name,
        })

    # Create a DataFrame from the extracted information
    df = pd.DataFrame(tracks_info)
    csv_file = "spotify_mage_data.csv"
    df.to_csv(csv_file, index=False)
    # Display the DataFrame
    print(df)
