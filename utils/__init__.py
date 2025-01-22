from .db_connection import connect_to_mongo
from .data_loader import load_data_from_mongo
from .data_processor import process_data
import logging

# Set up logging for the `utils` package
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Utils package loaded.")

# By doing this, you can import directly from `utils`:
# from utils import connect_to_mongo, load_data_from_mongo, process_data

