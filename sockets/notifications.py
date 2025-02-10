def handle_notifications(socketio):
    @socketio.on('notification')
    def handle_notification_event(data):
        print(f"Received notification: {data}")
        socketio.emit('notification_response', {"message": "Notification received"})
