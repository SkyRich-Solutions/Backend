from flask import Blueprint, jsonify
from utils.data_loader import load_data_from_mongo

unprocessed_blueprint = Blueprint('unprocessed', __name__)

@unprocessed_blueprint.route('/', methods=['GET'])
def get_unprocessed_data():
    try:
        df = load_data_from_mongo()
        return jsonify({
            "success": True,
            "data": df.to_dict(orient='records')
        }), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
