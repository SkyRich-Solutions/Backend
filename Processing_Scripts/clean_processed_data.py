def clean_processed_data(data):
    """Transform processed data for predictions and return success status."""
    if not data:
        return {"success": False, "message": "No processed data available"}
    
    transformed_data = [{**item, "predicted": True} for item in data]
    
    return {"success": True, "data": transformed_data}
