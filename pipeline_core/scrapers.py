import os
import logging
import requests
import pandas as pd
from datetime import datetime

logging.basicConfig(
    filename='logs/pipeline_execution.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ClimateDataPipeline:
    """
    Automated data engine that extracts, transforms, and logs 
    open-source climate stream JSON records into structured CSV datasets.
    """
    def __init__(self):
     self.api_url = "https://api.open-meteo.com/v1/forecast"
     self.params = {
         "latitude": 17.3850,
         "longitude": 78.4867,
         "current_weather": True
     }
     self.output_path = "output_data/current_metrics.csv"
       
    def fetch_live_stream(self):
        """Extracts raw JSON payloads from public sensor network endpoints with error safety."""
        try:
            logging.info("Initiating request to public climate endpoint stream...")
            response = requests.get(self.api_url, params=self.params, timeout=10)
            response.raise_for_status() 
            
            data = response.json()
            logging.info("Network payload extracted successfully.")
            return data.get("current_weather", {})
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Critical Pipeline Interruption: Network connection failure. Details: {e}")
            print(f"Extraction Error logged to system file: {e}")
            return None

    def execute_transform_and_load(self):
        """Transforms unstructured JSON dictionaries into clean tabular CSV schemas."""
        raw_payload = self.fetch_live_stream()
        if not raw_payload:
            return False

        raw_payload["timestamp_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
        df = pd.DataFrame([raw_payload])
        
        if not os.path.isfile(self.output_path):
            df.to_csv(self.output_path, index=False)
        else:
            df.to_csv(self.output_path, mode='a', header=False, index=False)
            
        logging.info(f"Tabular pipeline records written to file system disk at {self.output_path}")
        print(f"Pipeline executed. Record updated at: {raw_payload['timestamp_utc']}")
        return True

if __name__ == "__main__":
    pipeline = ClimateDataPipeline()
    pipeline.execute_transform_and_load()  
