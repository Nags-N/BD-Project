import requests
from flask import Flask, jsonify, request
import sys

app = Flask(__name__)

# Store the client's data
client_id = None

@app.route('/update', methods=['POST'])
def receive_update():
    """Receive updates from the subscriber."""
    data = request.json
    print(f"Received update: {data}")
    return jsonify({"message": "Data received"}), 200

@app.route('/register', methods=['POST'])
def register():
    """Register the client with a specific subscriber."""
    global client_id
    request_data = request.json

    if 'subscriber_id' not in request_data:
        return jsonify({"message": "subscriber_id missing in request"}), 400

    subscriber_id = request_data['subscriber_id']

    try:
        # Send a request to the registration service (e.g., server running on port 5001)
        response = requests.post('http://localhost:5001/clients/register', json={
            'subscriber_id': subscriber_id,
            'clients': [client_id]  # This is where the client ID gets registered
        })

        if response.status_code == 200:
            client_id = response.json().get('client_id')
            print(f"Client registered with ID: {client_id}")
            return jsonify({"message": "Client registered", "client_id": client_id}), 200
        else:
            print(f"Failed to register client: {response.status_code} - {response.text}")
            return jsonify({"message": "Failed to register"}), 400

    except requests.exceptions.RequestException as e:
        print(f"Error during registration: {e}")
        return jsonify({"message": "Error during registration"}), 500


if __name__ == "__main__":
    # Configurable client port
    PORT = 5002  # Default port
    if len(sys.argv) > 1:
        PORT = int(sys.argv[1])

    print(f"Client is running on port {PORT}")
    app.run(port=PORT)
