import os
import dotenv
import requests
import logging
from typing import List, Dict

dotenv.load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#API Configuration
API_URL = os.getenv('API_URL')
API_TOKEN = os.getenv('API_TOKEN')
HEADERS = {
    'Api-Token': API_TOKEN,
    'Content-Type': 'application/json'
}   

class ApiHandler:
    def __init__(self, endpoint: str, limit: int = 100):
        self.endpoint = endpoint
        self.limit = limit
        self.offset = 0
        self.all_data = []

    def set_endpoint(self, endpoint: str):
        self.endpoint = endpoint
        self.reset_offset()

    def fetch_data(self) -> List[Dict]:
        while True:
            try:
                url = f"{API_URL}{self.endpoint}?limit={self.limit}&offset={self.offset}"
                logger.info(f"Fetching data from {url}")
                response = requests.get(url, headers=HEADERS)
                
                data = response.json().get(self.endpoint, [])
                if not data:
                    break

                self.all_data.extend(data)
                self.offset += self.limit

                logger.info(f"Fetched {len(data)} records, total {len(self.all_data)} records")
            except Exception as e:
                logger.error(f"Error fetching data: {str(e)}")
                break
        return self.all_data
    
    def reset_offset(self):
        self.offset = 0
        self.all_data = []

        
