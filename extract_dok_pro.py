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
        endpoint = ".format(
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


# azure_connection_string = ""
container_name = "weather"  # Replace with your desired container name

blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client(container_name)

blob_name = "song_list"
try:
    blob_client = container_client.get_blob_client(blob_name)
    blob_data = blob_client.download_blob()
    existing_data = pd.read_csv(blob_data.content_as_text())
except Exception as e:
    print("Existing data not found:", e)
    existing_data = pd.DataFrame()

# Concatenate existing data with new data
all_data = pd.concat([existing_data, song_list], ignore_index=True)

# Upload the updated DataFrame to Azure Blob Storage
updated_blob_data = all_data.to_csv(index=False)
blob_client.upload_blob(updated_blob_data, blob_type="BlockBlob", overwrite=True)

print("Weather data has been successfully updated and uploaded to Azure Blob Storage.")
