from kafka import KafkaProducer
import random
import time
import json
import requests
from datetime import datetime

# Create a flag file to signal completion
with open('load_test_done.flag', 'w') as f:
    f.write('done')

# Kafka settings
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# List of emojis to simulate
emojis = ['🙂', '😃', '😢', '❤️', '🔥', '💥']

# Registration API URL
client_registration_url = 'http://localhost:5001/clients/'  # Adjust according to your setup

def get_registered_clients(subscriber_id):
    """Fetch the registered clients for the given subscriber."""
    try:
        response = requests.get(f'{client_registration_url}{subscriber_id}')
        if response.status_code == 200:
            return response.json().get('clients', [])
        else:
            print(f"Failed to fetch clients for {subscriber_id}: {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching registered clients for {subscriber_id}: {e}")
        return []

def send_emoji_data(subscriber_id):
    """Send emoji data only to registered clients."""
    # Fetch registered clients for the subscriber
    registered_clients = get_registered_clients(subscriber_id)
    
    if not registered_clients:
        print(f"No registered clients found for subscriber {subscriber_id}. Skipping emoji send.")
        return
    
    # Randomly select a client from the registered list
    client_id = random.choice(registered_clients)

    payload = {
        'user_id': client_id,  # Send emoji to a registered client
        'emoji_type': random.choice(emojis),  # Random emoji
        'timestamp': int(time.time() * 1000),  # Current timestamp in milliseconds
        'subscriber_id': subscriber_id  # Include the subscriber_id
    }

    # Send data to Kafka topic 'emoji-input'
    producer.send('emoji-input', payload)
    print(f"Sent to client {client_id} (subscriber {subscriber_id}): {payload}")

# Main loop to continuously send emoji data
if __name__ == '__main__':
    print("Starting load testing...")

    # Simulate sending data for multiple subscribers (e.g., subscriber-1, subscriber-2)
    subscriber_ids = ['subscriber-1', 'subscriber-2']  # List of subscribers

    while True:
        for subscriber_id in subscriber_ids:
            send_emoji_data(subscriber_id)
            time.sleep(3)  # Delay between requests
