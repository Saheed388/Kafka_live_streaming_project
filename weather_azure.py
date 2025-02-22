import requests
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient

def get_weather_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch weather data. Status Code: {response.status_code}")
        return None

# List of countries for which you want to fetch weather data
countries = ["Nigeria", "Ghana", "Benin", "Burkina Faso", "Cape Verde", "Ivory Coast", "Gambia", "Guinea", "Guinea-Bissau", "Liberia", "Mali", "Mauritania", "Niger", "Senegal", "Sierra Leone", "Togo"]

# Initialize an empty list to store weather data for different locations
all_weather_data = []

for country in countries:
    url = f""
    weather_data = get_weather_data(url)

    if weather_data:
        all_weather_data.append(weather_data)

# Create a DataFrame with all the weather data
westafrica_weather_data = pd.DataFrame()

for data in all_weather_data:
    westafrica_weather_data = westafrica_weather_data.append({
        "Name": data['location']['name'],
        "Region": data['location']['region'],
        "Country": data['location']['country'],
        "Latitude": data['location']['lat'],
        "Longitude": data['location']['lon'],
        "Timezone ID": data['location']['tz_id'],
        "Local Time Epoch": data['location']['localtime_epoch'],
        "Local Time": data['location']['localtime'],
        "Last Updated Epoch": data['current']['last_updated_epoch'],
        "Last Updated": data['current']['last_updated'],
        "Temperature (Celsius)": data['current']['temp_c'],
        "Temperature (Fahrenheit)": data['current']['temp_f'],
        "Is Day": data['current']['is_day'],
        "Condition": data['current']['condition']['text'],
        "Wind (mph)": data['current']['wind_mph'],
        "Wind (kph)": data['current']['wind_kph'],
        "Wind Degree": data['current']['wind_degree'],
        "Wind Direction": data['current']['wind_dir'],
        "Pressure (mb)": data['current']['pressure_mb'],
        "Pressure (in)": data['current']['pressure_in'],
        "Precipitation (mm)": data['current']['precip_mm'],
        "Precipitation (in)": data['current']['precip_in'],
        "Humidity": data['current']['humidity'],
        "Cloud Cover": data['current']['cloud'],
        "Feels Like (Celsius)": data['current']['feelslike_c'],
        "Feels Like (Fahrenheit)": data['current']['feelslike_f'],
        "Visibility (km)": data['current']['vis_km'],
        "Visibility (miles)": data['current']['vis_miles'],
        "UV Index": data['current']['uv'],
        "Gust (mph)": data['current']['gust_mph'],
        "Gust (kph)": data['current']['gust_kph']
    }, ignore_index=True)

# Add the current date and time to the DataFrame
current_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
westafrica_weather_data["Current Date and Time"] = current_date_time

# azure_connection_string = ""
container_name = "weather"  # Replace with your desired container name

blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client(container_name)

blob_name = "westafrica_weather_data.csv"
try:
    blob_client = container_client.get_blob_client(blob_name)
    blob_data = blob_client.download_blob()
    existing_data = pd.read_csv(blob_data.content_as_text())
except Exception as e:
    print("Existing data not found:", e)
    existing_data = pd.DataFrame()

# Concatenate existing data with new data
all_data = pd.concat([existing_data, westafrica_weather_data], ignore_index=True)

# Upload the updated DataFrame to Azure Blob Storage
updated_blob_data = all_data.to_csv(index=False)
blob_client.upload_blob(updated_blob_data, blob_type="BlockBlob", overwrite=True)

print("Weather data has been successfully updated and uploaded to Azure Blob Storage.")
