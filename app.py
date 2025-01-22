from flask import Flask
from flask_cors import CORS
from flask_socketio import SocketIO
from routes.unprocessed import unprocessed_blueprint
from routes.processing import processing_blueprint
from routes.processed import processed_blueprint
from sockets.notifications import handle_notifications
from sockets.data_events import handle_data_events
from utils import connect_to_mongo, load_data_from_mongo, process_data

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize Flask-SocketIO
socketio = SocketIO(app, cors_allowed_origins="*")

# Test MongoDB connection during startup
try:
    unprocessed_collection, processed_collection = connect_to_mongo()
    print("Connected to MongoDB successfully!")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    exit(1)  # Exit the app if MongoDB connection fails

# Register Blueprints (RESTful endpoints)
app.register_blueprint(unprocessed_blueprint, url_prefix='/unprocessed')
app.register_blueprint(processing_blueprint, url_prefix='/process')
app.register_blueprint(processed_blueprint, url_prefix='/processed')

# Register SocketIO Event Handlers
handle_notifications(socketio)
handle_data_events(socketio)

if __name__ == '__main__':
    socketio.run(app, port=5000, debug=True)
