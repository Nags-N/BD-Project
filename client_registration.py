from flask import Flask, request, jsonify
from collections import defaultdict

app = Flask(__name__)

# A dictionary to keep track of subscribers and their registered clients
subscribers = defaultdict(list)

@app.route('/register', methods=['POST'])
def register_client():
    """Register a client to a subscriber"""
    data = request.get_json()
    subscriber_id = data.get('subscriber_id')
    client_id = data.get('client_id')

    if not subscriber_id or not client_id:
        return jsonify({"error": "Missing subscriber_id or client_id"}), 400

    # Add client to the subscriber
    subscribers[subscriber_id].append(client_id)
    return jsonify({"message": f"Client {client_id} registered to {subscriber_id}"}), 200


@app.route('/unregister', methods=['POST'])
def unregister_client():
    """Unregister a client from a subscriber"""
    data = request.get_json()
    subscriber_id = data.get('subscriber_id')
    client_id = data.get('client_id')

    if not subscriber_id or not client_id:
        return jsonify({"error": "Missing subscriber_id or client_id"}), 400

    if client_id in subscribers[subscriber_id]:
        subscribers[subscriber_id].remove(client_id)
        return jsonify({"message": f"Client {client_id} unregistered from {subscriber_id}"}), 200
    else:
        return jsonify({"error": f"Client {client_id} is not registered under {subscriber_id}"}), 400


@app.route('/clients/<subscriber_id>', methods=['GET'])
def get_clients(subscriber_id):
    """Get list of clients registered under a subscriber"""
    clients = subscribers.get(subscriber_id, [])
    return jsonify({"subscriber_id": subscriber_id, "clients": clients}), 200


if __name__ == '__main__':
    app.run(host="localhost", port=5001, debug=True)
