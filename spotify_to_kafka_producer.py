import json
import requests
import os
import pandas as pd
from secret_file import spotify_user_id
from datetime import datetime, timedelta
from refresh_token import Refresh
from kafka import KafkaProducer
import csv
import time
import logging
from kafka.errors import KafkaError

class SaveSongs:
    def __init__(self):
        self.user_id = spotify_user_id
        self.spotify_token = ""
        self.tracks = ""

    def get_recently_played(self):
        today = datetime.now()
        past_7_days = today - timedelta(days=8)
        past_7_days_unix_timestamp = int(past_7_days.timestamp()) * 1000

    def call_refresh(self):
        print("Refreshing token")
        refreshCaller = Refresh()
        self.spotify_token = refreshCaller.refresh()

def send_data():
    # Initialize Kafka producer
    kafka_bootstrap_servers = 'kafka-broker-url:port'  # Update with actual Kafka broker address
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    a = SaveSongs()
    a.call_refresh()
    data = a.get_recently_played()

    # Process and publish data to Kafka topic
    kafka_topic = 'spotify_topic'

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
        producer.send('spotify_topic', value=song_data)
    
    producer.close()

# Entry point of the script
if __name__ == "__main__":
    send_data()
