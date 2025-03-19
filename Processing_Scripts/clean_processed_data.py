def clean_processed_data(data):
    """Transform processed data for predictions."""
    return [{**item, "predicted": True} for item in data]
