def handle_data_events(socketio):
    @socketio.on('start_processing')
    def handle_start_processing(data):
        print("Processing started...")
        for i in range(1, 6):
            socketio.emit('processing_update', {'step': i, 'status': f'Step {i} completed'})
