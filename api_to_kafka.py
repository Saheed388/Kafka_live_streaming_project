from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from refresh_token import Refresh
from kafka import KafkaProducer
import requests 
import os
import pandas as pd
from secret_file import refresh_token, base_64, spotify_user_id


def push_spotify_data_to_kafka():
    class SaveSongs:
        def __init__(self):
            self.user_id = 'your_spotify_user_id'
            self.spotify_token = ""
            self.tracks = ""

        def get_recently_played(self):
            # Convert time to Unix timestamp in milliseconds
            today = datetime.now()
            past_7_days = today - timedelta(days=8)
            past_7_days_unix_timestamp = int(past_7_days.timestamp()) * 1000

            # Your code...

        def call_refresh(self):
            print("Refreshing token")
            refreshCaller = Refresh()
            self.spotify_token = refreshCaller.refresh()

        def push_to_kafka(self, topic, message):
            kafka_config = {
                'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
                # Additional Kafka configuration options if needed
            }

            producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    producer.send(topic, value=message)
    producer.flush()


    a = SaveSongs()
    a.call_refresh()

    data = a.get_recently_played()
    print("Keys in data:", data.keys())  # Print keys in data

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

    # Convert DataFrame to JSON
    json_data = df.to_json(orient="records")

    # Push JSON data to Kafka topic
    topic_name = 'spotify_topic'  # Replace with your Kafka topic name
    a.push_to_kafka(topic_name, json_data)

    print("Data has been successfully pushed to Kafka topic:", topic_name)

# Define your default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG instance
with DAG(
    'spotify_data_to_kafka',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Set the frequency as needed
    catchup=False,
) as dag:

    # Define a PythonOperator that will execute your function
    push_data_task = PythonOperator(
        task_id='push_spotify_data',
        python_callable=push_spotify_data_to_kafka,
    )
