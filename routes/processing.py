from flask import Blueprint, jsonify
from utils.data_processor import process_data

processing_blueprint = Blueprint('processing', __name__)

@processing_blueprint.route('/', methods=['POST'])
def process_data_route():
    try:
        processed_df, discrepancies_df = process_data()
        return jsonify({
            "success": True,
            "message": "Data processed successfully.",
            "processed_data_count": len(processed_df),
            "discrepancies": discrepancies_df.to_dict(orient='records')
        }), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
