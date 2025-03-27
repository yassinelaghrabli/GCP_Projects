import functions_framework
from google.cloud import pubsub_v1
import json
import os
import requests

functions_framework.http
def publish_bike_data(request):
    PROJECT_ID = os.environ.get("PROJECT_ID")
    TOPIC = os.environ.get("TOPIC")
    API_KEY = os.environ.get("API_KEY")
    CONTRACT_NAME = "toulouse"

    if not (PROJECT_ID and TOPIC and API_KEY):
        return "Missing environment variables", 500

    url = f"https://api.jcdecaux.com/vls/v1/stations?contract={CONTRACT_NAME}&apiKey={API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        stations = response.json()
    except Exception as e:
        return f"Failed to fetch data: {e}", 500

    publisher = pubsub_v1.PublisherClient()
    topic_path = TOPIC

    for station in stations:
        message = json.dumps(station).encode("utf-8")
        publisher.publish(topic_path, message)

    return f"{len(stations)} messages published", 200
