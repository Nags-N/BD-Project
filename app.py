from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import time

app = Flask(__name__)

# Kafka topic
TOPIC_NAME = 'emoji-input'

# Kafka Producer Setup
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer connected successfully.")
except Exception as e:
    print("Error connecting to Kafka:", e)
    producer = None

# Simulated in-memory client registration (to be updated dynamically)
registered_clients = {
    "subscriber-1": [1, 2, 3],
    "subscriber-2": [4, 5, 6],
}
session_start_time = int(time.time())  # Track session start time


# Endpoint to fetch registered clients for a subscriber
@app.route('/clients/<subscriber_id>', methods=['GET'])
def get_clients(subscriber_id):
    """Returns the list of clients registered for a specific subscriber."""
    clients = registered_clients.get(subscriber_id, [])
    if clients:
        return jsonify({'clients': clients}), 200
    else:
        return jsonify({'error': 'Subscriber not found or no registered clients'}), 404


# Endpoint to register or update clients for a subscriber
@app.route('/clients/register', methods=['POST'])
def register_clients():
    """Registers new clients or updates the list of clients for a subscriber."""
    try:
        data = request.json

        # Validate input
        if not data or not isinstance(data, dict):
            return jsonify({'error': 'Invalid data format'}), 400

        subscriber_id = data.get('subscriber_id')
        clients = data.get('clients')

        if not subscriber_id or not isinstance(clients, list):
            return jsonify({'error': 'Invalid data structure'}), 400

        # Update registered clients for the subscriber
        if subscriber_id not in registered_clients:
            registered_clients[subscriber_id] = []

        # Add clients to the subscriber's list
        registered_clients[subscriber_id].extend(clients)

        # Remove duplicates in the list
        registered_clients[subscriber_id] = list(set(registered_clients[subscriber_id]))

        print(f"Registered clients updated: {registered_clients}")
        return jsonify({'message': f"Clients registered for {subscriber_id}"}), 200
    except Exception as e:
        print(f"Error registering clients: {e}")
        return jsonify({'error': str(e)}), 500


# Endpoint to unregister clients from a subscriber
@app.route('/clients/unregister', methods=['POST'])
def unregister_clients():
    """Unregisters clients from a specific subscriber."""
    try:
        data = request.json

        # Validate input
        if not data or not isinstance(data, dict):
            return jsonify({'error': 'Invalid data format'}), 400

        subscriber_id = data.get('subscriber_id')
        clients = data.get('clients')

        if not subscriber_id or not isinstance(clients, list):
            return jsonify({'error': 'Invalid data structure'}), 400

        # Check if subscriber exists
        if subscriber_id not in registered_clients:
            return jsonify({'error': 'Subscriber not found'}), 404

        # Remove clients from the subscriber's list
        registered_clients[subscriber_id] = [client for client in registered_clients[subscriber_id] if client not in clients]

        print(f"Updated client list after removal: {registered_clients}")
        return jsonify({'message': f"Clients unregistered from {subscriber_id}"}), 200
    except Exception as e:
        print(f"Error unregistering clients: {e}")
        return jsonify({'error': str(e)}), 500


# Endpoint to receive emoji data from clients
@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    """Receives emoji data from clients and sends it to Kafka if valid."""
    if not producer:
        return jsonify({'error': 'Kafka producer is not available'}), 500

    try:
        data = request.json

        # Validate input
        if not data or not isinstance(data, dict):
            return jsonify({'error': 'Invalid data format'}), 400

        required_keys = ['user_id', 'emoji_type', 'timestamp']
        for key in required_keys:
            if key not in data:
                return jsonify({'error': f"Missing required field: {key}"}), 400

        # Ensure correct data types
        if not isinstance(data['user_id'], int) or not isinstance(data['emoji_type'], str) or not isinstance(data['timestamp'], int):
            return jsonify({'error': 'Invalid data types'}), 400

        # Verify the client is registered under any subscriber
        is_registered = any(data['user_id'] in clients for clients in registered_clients.values())
        if not is_registered:
            return jsonify({'error': 'User is not registered'}), 403

        # Ensure the emoji is sent only in the current session
        if data['timestamp'] < session_start_time:
            return jsonify({'error': 'Data from a previous session cannot be processed'}), 400

        # Send data to Kafka
        producer.send(TOPIC_NAME, data)
        producer.flush()
        print(f"Data sent to Kafka: {data}")
        return jsonify({'message': 'Data sent to Kafka successfully'}), 200
    except Exception as e:
        print(f"Error while sending data to Kafka: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
