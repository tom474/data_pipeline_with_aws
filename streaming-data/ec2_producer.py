import requests
from datetime import datetime, timedelta
import time
import json
import boto3

OWM_API_KEY = "8be8123961bf1e8135acf22a36c65d8b"
KINESIS_STREAM_NAME = "weather-data-stream"

# Initialize Kinesis client
kinesis_client = boto3.client("kinesis", region_name="us-east-1")

# Airport-to-city dictionary
AIRPORTS = {
    "ATL": "Atlanta",
    "LAX": "Los Angeles",
    "JFK": "New York",
    "LGA": "New York",
    "MDW": "Chicago",
    "ORD": "Chicago"
}

# Logging function
def log_message(message):
    current_time_utc = datetime.utcnow()
    current_time_local = current_time_utc + timedelta(hours=7)
    formatted_time = current_time_local.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{formatted_time}] {message}")
    

# Get coordinates for a city
def get_long_lat(city):
    try:
        url = f"https://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={OWM_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if data:
            return data[0].get("lat"), data[0].get("lon")
        else:
            log_message(f"No data found for {city}.")
            return None, None
        
    except requests.exceptions.RequestException as e:
        log_message(f"Error getting long/lat for {city}: {e}")
        return None, None

# Get weather data for a specific date
def get_weather_on_date(airports, date):
    all_weather_data = []

    for airport_code in airports:
        city = AIRPORTS.get(airport_code)
        if not city:
            log_message(f"Airport code {airport_code} not found in the dictionary. Skipping...")
            continue

        lat, long = get_long_lat(city)
        if lat is None or long is None:
            log_message(f"Could not fetch coordinates for {city}. Skipping...")
            continue

        try:
            # Fetch weather data from OpenWeatherMap
            url = f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lat}&lon={long}&date={date}&appid={OWM_API_KEY}&units=metric"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            temp_avg = (data["temperature"]["max"] + data["temperature"]["min"]) / 2
            temp_min = data["temperature"]["min"]
            temp_max = data["temperature"]["max"]
            precipitation = data["precipitation"]["total"]
            snow_depth = data["snow"] if "snow" in data else 0
            wind_dir = data["wind"]["max"]["direction"]
            wind_speed = data["wind"]["max"]["speed"] * 3.6
            pressure = data["pressure"]["afternoon"]

            # Append the weather data for the current airport
            all_weather_data.append({
                "time": date,
                "tavg": round(temp_avg, 2),
                "tmin": temp_min,
                "tmax": temp_max,
                "prcp": precipitation,
                "snow": snow_depth,
                "wdir": wind_dir,
                "wspd": round(wind_speed, 2),
                "pres": pressure,
                "airport_id": airport_code
            })

        except requests.exceptions.RequestException as e:
            log_message(f"Error getting weather for {city} on {date}: {e}")
            continue

    return all_weather_data

# Get daily weather forecast for a list of airports
def get_daily_weather(airports):
    all_weather_data = []

    for airport_code in airports:
        city = AIRPORTS.get(airport_code)
        if not city:
            log_message(f"Airport code {airport_code} not found in the dictionary. Skipping...")
            continue

        lat, long = get_long_lat(city)
        if lat is None or long is None:
            log_message(f"Could not fetch coordinates for {city}. Skipping...")
            continue

        try:
            # Fetch daily weather data from OpenWeatherMap
            url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={long}&exclude=current,minutely,hourly,alerts&appid={OWM_API_KEY}&units=metric"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            daily_data = data.get("daily", [])
            for day in daily_data:
                date = datetime.utcfromtimestamp(day["dt"]).strftime('%Y-%m-%d')
                temp_avg = (day["temp"]["max"] + day["temp"]["min"]) / 2
                temp_min = day["temp"]["min"]
                temp_max = day["temp"]["max"]
                precipitation = day["rain"] if "rain" in day else 0
                snow_depth = day["snow"] if "snow" in day else 0
                wind_dir = day["wind_deg"]
                wind_speed = day["wind_speed"] * 3.6
                pressure = day["pressure"]

                all_weather_data.append({
                    "time": date,
                    "tavg": round(temp_avg, 2),
                    "tmin": temp_min,
                    "tmax": temp_max,
                    "prcp": precipitation,
                    "snow": snow_depth,
                    "wdir": wind_dir,
                    "wspd": round(wind_speed, 2),
                    "pres": pressure,
                    "airport_id": airport_code
                })
        
        except requests.exceptions.RequestException as e:
            log_message(f"Error getting weather for {city}: {e}")
            continue

    return all_weather_data
    

# Send data to Kinesis
def put_records_to_kinesis(stream_name, records):
    for record in records:
        try:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(record),
                PartitionKey=record["airport_id"]
            )
            log_message(f"Record sent to Kinesis. SequenceNumber: {response['SequenceNumber']}")
            print("Record details: ")
            print(json.dumps(record))
        except Exception as e:
            log_message(f"Failed to send record to Kinesis: {e}")


# Main logic
if __name__ == "__main__":
    airports = ["ATL", "LAX", "JFK", "LGA", "MDW", "ORD"]

    while True:
        # Fetch and send daily weather data
        daily_weather = get_daily_weather(airports)
        if daily_weather:
            log_message(f"Sending daily weather data to Kinesis...")
            put_records_to_kinesis(KINESIS_STREAM_NAME, daily_weather)
        else:
            log_message(f"No data found.")

        # Wait for 1 hour before the next execution
        time.sleep(60 * 60)
        