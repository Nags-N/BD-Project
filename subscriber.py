import requests
from kafka import KafkaConsumer
import json
import time
import threading

# Configuration
input_topic = "emoji-cluster"  # Kafka topic subscribed to
kafka_broker = "localhost:9092"  # Kafka broker address
subscriber_id = "subscriber-1"  # Unique identifier for this subscriber
registered_clients = []  # List of clients registered to this subscriber
registration_service_url = f"http://localhost:5001/clients/{subscriber_id}"

# Fetch registered clients from the registration service
def fetch_registered_clients():
    """Fetch the list of registered clients for this subscriber."""
    try:
        response = requests.get(registration_service_url)
        if response.status_code == 200:
            clients = response.json().get('clients', [])
            print(f"Fetched registered clients: {clients}")
            return clients
        else:
            print(f"Error fetching registered clients: {response.status_code} - {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching registered clients: {e}")
        return []

# Update registered clients periodically
def periodically_update_clients(interval=10):
    """Periodically update the list of registered clients."""
    global registered_clients
    while True:
        registered_clients = fetch_registered_clients()
        time.sleep(interval)

# Initialize Kafka consumer
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=kafka_broker,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=f'{subscriber_id}-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Start a thread to update the clients list periodically
client_update_thread = threading.Thread(target=periodically_update_clients, daemon=True)
client_update_thread.start()

# Indicate that the subscriber is running
print(f"Subscriber '{subscriber_id}' is running...")

# Process incoming Kafka messages
for message in consumer:
    aggregated_data = message.value
    print(f"Received aggregated data: {aggregated_data}")

    # Send the data to all registered clients
    for client_id in registered_clients:
        try:
            client_url = f"http://localhost:5002/update/{client_id}"  # Endpoint for specific client
            response = requests.post(client_url, json=aggregated_data, timeout=5)
            if response.status_code == 200:
                print(f"Data successfully sent to client {client_id}")
            else:
                print(f"Failed to send data to client {client_id}: {response.status_code} - {response.text}")
        except requests.exceptions.Timeout:
            print(f"Request to client {client_id} timed out.")
        except requests.exceptions.RequestException as e:
            print(f"Error sending data to client {client_id}: {e}")

    # Optional: Throttle the requests to clients
    time.sleep(0.5)  # Adjust the sleep duration as needed
