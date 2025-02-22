import json
import requests
import os
import pandas as pd
from secret_file import spotify_user_id
from datetime import datetime, timedelta
from refresh_token import Refresh

class SaveSongs:
    def __init__(self):
        self.user_id = spotify_user_id
        self.spotify_token = ""
        self.tracks = ""

    def get_recently_played(self):
        # Convert time to Unix timestamp in milliseconds
        today = datetime.now()
        past_7_days = today - timedelta(days=8)
        past_7_days_unix_timestamp = int(past_7_days.timestamp()) * 1000

        # Download all songs you've listened to "after yesterday," which means in the last 24 hours
        endpoint = "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(
            time=past_7_days_unix_timestamp)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.spotify_token)
        }
        r = requests.get(endpoint, headers=headers, params={"limit": 50})
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

    data = a.get_recently_played()
    print(data.keys())

    song_list = []

    for song_item in data["items"]:
        track_info = song_item["track"]
        song_data = {
            "song_name": track_info["name"],
            "artist_name": track_info["artists"][0]["name"],
            "featured_artists": [artist["name"] for artist in track_info["artists"][1:]],
            "played_at": song_item["played_at"],
            "timestamp": song_item["played_at"][0:10],
            "popularity": track_info["popularity"],
            "album_or_single": track_info["album"]["album_type"]
        }
        song_list.append(song_data)

    # Convert song_list to a DataFrame
    df = pd.DataFrame(song_list)

    # Save the DataFrame as a CSV file
    df.to_csv("spotify_data.csv", index=False)


