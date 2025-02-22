import json
import requests
from secret_file import spotify_user_id
from datetime import datetime, timedelta
from refresh_token import Refresh

class SaveSongs:
    def __init__(self):
        self.user_id = spotify_user_id
        self.spotify_token = ""
        self.tracks = []

    def get_recently_played(self):
        # Convert time to Unix timestamp in milliseconds
        past_7_days_unix_timestamp = int((datetime.now() - timedelta(days=7)).timestamp()) * 1000
        endpoint = f"https://api.spotify.com/v1/me/player/recently-played?after={past_7_days_unix_timestamp}"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.spotify_token}"
        }
        r = requests.get(endpoint, headers=headers, params={"limit": 50})
        r.raise_for_status()  # Raise an exception if the request was not successful
        return r.text  # Return the raw JSON response as a string

    def call_refresh(self):
        print("Refreshing token")
        refresh_caller = Refresh()
        self.spotify_token = refresh_caller.refresh()

a = SaveSongs()
a.call_refresh()

json_response = a.get_recently_played()

print(json_response)
