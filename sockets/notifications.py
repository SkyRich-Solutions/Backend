def handle_notifications(socketio):
    @socketio.on('connect')
    def handle_connect():
        print("Client connected")
        socketio.emit('message', {'message': 'Welcome to Flask-SocketIO!'})

    @socketio.on('disconnect')
    def handle_disconnect():
        print("Client disconnected")

    @socketio.on('send_notification')
    def handle_notification(data):
        print(f"Notification received: {data}")
        socketio.emit('notification', {'data': data})
