from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError
import os
import dotenv
import pandas as pd
import pandas_gbq
import logging

dotenv.load_dotenv()
logging.basicConfig(
    level=logging.info,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_credentials():
    try:
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        return credentials
    
    except Exception as e:
        logger.error(f"Error getting credentials: {str(e)}")
        raise

def save_to_bigquery(data, project_id, dataset_id, table_id):
    if not data:
        logger.warning("No data to upload")
        return
    if not all([project_id, dataset_id, table_id]):
        logger.warning("Missing required parameters")
    
    try:
        credentials = get_credentials()
        df = pd.DataFrame(data)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        pandas_gbq.to_gbq(
            df, 
            destination_table=table_ref,
            if_exists='replace',
            credentials=credentials, 
            project_id=project_id,
            progress_bar=True
            )
        logger.info(f"Data successfully uploaded to {table_ref}")
    except GoogleAPICallError as e:
        logger.error(f"Error uploading data to BigQuery: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error saving data to BigQuery: {str(e)}")
        raise
