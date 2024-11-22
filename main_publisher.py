from kafka import KafkaConsumer, KafkaProducer
import json
import time
import requests

# Configuration
input_topic = "emoji-main"  # Topic from Spark Stream
output_topic = "cluster-publisher"  # Topic for Cluster Publisher
kafka_broker = "localhost:9092"  # Kafka broker address
client_registration_url = 'http://localhost:5001/clients/'  # Registration URL

# Initialize Kafka consumer to read aggregated data from Spark
try:
    consumer = KafkaConsumer(
        input_topic,
        bootstrap_servers=kafka_broker,
        auto_offset_reset='latest',  # Read only new data from the latest offset
        enable_auto_commit=True,
        group_id='main-publisher-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(f"Connected to Kafka as consumer on topic '{input_topic}'")
except Exception as e:
    print(f"Error connecting to Kafka as consumer: {e}")
    consumer = None

# Initialize Kafka producer to forward aggregated data
try:
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print(f"Connected to Kafka as producer on topic '{output_topic}'")
except Exception as e:
    print(f"Error connecting to Kafka as producer: {e}")
    producer = None

def get_registered_clients(subscriber_id):
    """Fetch the registered clients for a given subscriber."""
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

if consumer and producer:
    print("Main Publisher is running...")
    try:
        while True:
            # Poll for messages from Kafka
            message_batch = consumer.poll(timeout_ms=1000)  # Poll every 1 second
            
            for _, messages in message_batch.items():
                for message in messages:
                    aggregated_data = message.value
                    print(f"Received aggregated data: {aggregated_data}")

                    # Ensure that the aggregated data is valid before forwarding
                    if aggregated_data and 'window_start' in aggregated_data and 'window_end' in aggregated_data:
                        # Get the subscriber_id from the aggregated data (assuming it's included)
                        subscriber_id = aggregated_data.get('subscriber_id')
                        
                        if subscriber_id:
                            # Fetch the registered clients for the subscriber
                            registered_clients = get_registered_clients(subscriber_id)
                            
                            if registered_clients:
                                for client_id in registered_clients:
                                    # Create payload for each client
                                    payload = {
                                        'subscriber_id': subscriber_id,
                                        'client_id': client_id,
                                        'data': aggregated_data
                                    }
                                    try:
                                        # Send the data to the cluster-publisher topic
                                        producer.send(output_topic, payload)
                                        producer.flush()  # Ensure the message is sent immediately
                                        print(f"Forwarded aggregated data to client {client_id}: {payload}")
                                    except Exception as e:
                                        print(f"Error forwarding data to client {client_id}: {e}")
                            else:
                                print(f"No registered clients found for subscriber {subscriber_id}, skipping data forwarding.")
                        else:
                            print("No subscriber_id found in aggregated data, skipping forwarding.")
                    else:
                        print("Invalid aggregated data received, skipping forward.")

            # Optional: Limit the rate of consuming/producing
            time.sleep(1)

    except KeyboardInterrupt:
        print("Main Publisher stopped.")
    except Exception as e:
        print(f"Error in main loop: {e}")
    finally:
        # Ensure both consumer and producer are closed properly
        if consumer:
            consumer.close()
        if producer:
            producer.close()
else:
    print("Main Publisher could not start due to Kafka connection issues.")
