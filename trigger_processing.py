from utils.data_processor import process_data
import json

if __name__ == "__main__":
    try:
        processed_data, discrepancies = process_data()

        # Return JSON output to be read by Node.js
        print(json.dumps({
            "processed_count": len(processed_data),
            "discrepancy_count": len(discrepancies),
            "discrepancies": discrepancies.to_dict('records')
        }))
    except Exception as e:
        print(json.dumps({
            "error": str(e)
        }))
