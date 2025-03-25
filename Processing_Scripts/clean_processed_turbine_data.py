def clean_processed_turbine_data(data):
    if data.empty:  # ✅ Fix: Use .empty instead of "if not data"
        return {"success": False, "data": []}

    # Your existing cleaning logic...
    
    return {"success": True, "data": data.to_dict(orient="records")}
