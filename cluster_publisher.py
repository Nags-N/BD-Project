import json
import signal
import sys
from kafka import KafkaConsumer, KafkaProducer
import logging
import requests

# Configuration
input_topic = "cluster-publisher"  # Topic from the main publisher
output_topic = "emoji-cluster"  # Topic for cluster subscribers
kafka_broker = "localhost:9092"
client_registration_url = 'http://localhost:5001/clients/'  # Endpoint to get registered clients

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Kafka consumer
try:
    consumer = KafkaConsumer(
        input_topic,
        bootstrap_servers=kafka_broker,
        auto_offset_reset='latest',  # Start consuming only new messages
        enable_auto_commit=True,  # Commit offsets automatically
        group_id='cluster-publisher-group',  # Consumer group ID
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON data
    )
    logging.info(f"Connected to Kafka consumer on topic '{input_topic}'")
except Exception as e:
    logging.error(f"Error initializing KafkaConsumer: {e}")
    sys.exit(1)

# Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Serialize JSON data
    )
    logging.info(f"Connected to Kafka producer on topic '{output_topic}'")
except Exception as e:
    logging.error(f"Error initializing KafkaProducer: {e}")
    consumer.close()
    sys.exit(1)

# Graceful shutdown
def shutdown_handler(signal_received, frame):
    logging.info("Shutting down Cluster Publisher...")
    consumer.close()
    producer.close()
    logging.info("Cluster Publisher shut down successfully.")
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# Function to get registered clients for a subscriber
def get_registered_clients(subscriber_id):
    """Fetch the registered clients for the given subscriber."""
    try:
        response = requests.get(f'{client_registration_url}{subscriber_id}')
        if response.status_code == 200:
            return response.json().get('clients', [])
        else:
            logging.warning(f"Failed to fetch clients for {subscriber_id}: {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching registered clients for {subscriber_id}: {e}")
        return []

# Consume and publish messages
logging.info("Cluster Publisher is running...")

try:
    for message in consumer:
        aggregated_data = message.value
        logging.info(f"Received data from main publisher: {aggregated_data}")

        # Extract subscriber_id and emoji_data
        subscriber_id = aggregated_data.get('subscriber_id')
        if not subscriber_id:
            logging.warning("No subscriber_id found in the message. Skipping message.")
            continue

        # Get the list of registered clients for this subscriber
        registered_clients = get_registered_clients(subscriber_id)

        # Ensure valid data before forwarding
        if aggregated_data and 'emoji_type' in aggregated_data and 'count' in aggregated_data:
            if not registered_clients:
                logging.warning(f"No registered clients for subscriber {subscriber_id}. Skipping message.")
                continue

            # Forward data only to registered clients
            for client in registered_clients:
                data_to_send = aggregated_data.copy()
                data_to_send['client_id'] = client  # Add the client_id to the data

                try:
                    # Forward data to the cluster subscribers
                    producer.send(output_topic, data_to_send)
                    producer.flush()  # Ensure the message is sent immediately
                    logging.info(f"Forwarded data to topic '{output_topic}' for client {client}: {data_to_send}")
                except Exception as e:
                    logging.error(f"Error forwarding data to topic '{output_topic}' for client {client}: {e}")
        else:
            logging.warning(f"Invalid data received: {aggregated_data}")

except KeyboardInterrupt:
    logging.info("Cluster Publisher stopped.")
except Exception as e:
    logging.error(f"Error in main loop: {e}")
finally:
    consumer.close()
    producer.close()
    logging.info("Cluster Publisher shut down.")
