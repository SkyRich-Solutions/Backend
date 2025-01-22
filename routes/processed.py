from flask import Blueprint

processed_blueprint = Blueprint('processed', __name__)

@processed_blueprint.route('/')
def processed_home():
    return {"message": "Processed endpoint is working!"}
