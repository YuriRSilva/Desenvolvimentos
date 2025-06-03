from api_handler import ApiHandler
from bq_handler import save_to_bigquery
import dotenv
import os
import logging

dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

api_endpoint = "lists" #Change to the endpoint you want to use

project_id = os.getenv('PROJECT_ID')
dataset_id = os.getenv('DATASET_ID')
table_id = f"ac_{api_endpoint}"

if __name__ == "__main__":
    try:
        logger.info("Starting data fetching process")
        api_handler = ApiHandler(limit=100, endpoint=api_endpoint)
        data = api_handler.fetch_data()

        if data:
            logger.info(f"Fetched {len(data)} records")
            save_to_bigquery(data, project_id, dataset_id, table_id)
        else:
            logger.warning("No data fetched")
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        raise
    finally:
        api_handler.reset_offset()