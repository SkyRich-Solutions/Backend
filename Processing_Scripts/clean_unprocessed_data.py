def clean_unprocessed_data(data):
    """Process data by adding a 'cleaned' flag and any other transformations."""
    return [{**item, "cleaned(Unprocessed)": True} for item in data]
