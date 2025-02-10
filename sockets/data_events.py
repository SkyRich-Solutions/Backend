from flask_socketio import SocketIO, emit
from datetime import datetime

def handle_data_events(socketio):
    @socketio.on("connect")
    def handle_connect():
        print("🟢 Client connected.")

    @socketio.on("disconnect")
    def handle_disconnect():
        print("🔴 Client disconnected.")

    @socketio.on("send_message")
    def handle_send_message(data):
        """Receive a message, add timestamp, and broadcast it."""
        message = data.get("message", "")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"📩 Received message: {message} at {timestamp}")

        # Send message to all connected clients
        emit("receive_message", {"message": message, "timestamp": timestamp}, broadcast=True)

def handle_data_events(socketio):
    @socketio.on('data_request')
    def handle_data_request(data):
        print(f"Received data request: {data}")
        socketio.emit('data_response', {"message": "Data processed"})
