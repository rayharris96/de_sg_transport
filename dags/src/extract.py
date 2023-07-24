import requests
import os
import json
from datetime import datetime
from .utils import load_env, upload_file_to_s3, download_latest_file_from_s3


def call_lta_bus_api():
    """
    Function that calls LTA bus api 
    Stores data into S3
    """
    RAW_BUCKET = 'raw-kungfu-challenge'
    PREFIX = 'bus_stop'
    LTA_API_KEY = os.environ.get('LTA_API_KEY')
    BUS_STOP_NO=81111

    url = f"http://datamall2.mytransport.sg/ltaodataservice/BusArrivalv2?BusStopCode={BUS_STOP_NO}"

    payload = {}
    headers = {
    'x-api-version': 'v1',
    'Accept': 'application/json',
    'AccountKey': LTA_API_KEY,
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code == 200:
        data = response.json()
        # Process the data as needed
        # ...

        # Define the directory for storing data
        data_dir = './data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Format the current time as a string and use it in the filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = os.path.join(data_dir, f'data_bus_{timestamp}.json')

        # Write data to a file in the directory
        with open(file_path, 'w') as file:
            json.dump(data, file)
            
        print(f"Data written to {file_path}")
        
    else:
        raise Exception(f"API request failed with status {response.status_code}")

    #Dump data to S3 bucket
    upload_file_to_s3(RAW_BUCKET, PREFIX, file_path)
    print(f"{file_path} uploaded to {RAW_BUCKET}/{PREFIX}")
    
    #Delete data after upload
    if os.path.exists(file_path):
        os.remove(file_path)
    print(f"{file_path} removed from local")


def call_erp_api():
    """
    Function that calls LTA bus api
    Stores data into S3
    """
    RAW_BUCKET = 'raw-kungfu-challenge'
    PREFIX_ERP = 'erp_rate'
    LTA_API_KEY = os.environ.get('LTA_API_KEY')

    url = f'http://datamall2.mytransport.sg/ltaodataservice/ERPRates'

    payload = {}
    headers = {
        'x-api-version': 'v1',
        'Accept': 'application/json',
        'AccountKey': LTA_API_KEY,
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code == 200:
        data = response.json()
        # Process the data as needed
        # ...

        # Define the directory for storing data
        data_dir = './data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Format the current time as a string and use it in the filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = os.path.join(data_dir, f'data_erp_{timestamp}.json')

        # Write data to a file in the directory
        with open(file_path, 'w') as file:
            json.dump(data, file)

        print(f"Data written to {file_path}")

    else:
        raise Exception(f"API request failed with status {response.status_code}")

    # Dump data to S3 bucket
    upload_file_to_s3(RAW_BUCKET, PREFIX_ERP, file_path)
    print(f"{file_path} uploaded to {RAW_BUCKET}/{PREFIX_ERP}")

    # Delete data after upload
    if os.path.exists(file_path):
        os.remove(file_path)
    print(f"{file_path} removed from local")


def call_taxi_api():
    """
    Function that calls LTA taxi api 
    Stores data into S3
    """
    RAW_BUCKET = 'raw-kungfu-challenge'
    PREFIX = 'taxi'
    LTA_API_KEY = os.environ.get('LTA_API_KEY')

    url = "http://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability"
    payload = {}
    headers = {
    'x-api-version': 'v1',
    'Accept': 'application/json',
    'AccountKey': LTA_API_KEY,
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code == 200:
        data = response.json()
        # Process the data as needed
        # ...

        # Define the directory for storing data
        data_dir = './data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Format the current time as a string and use it in the filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = os.path.join(data_dir, f'data_taxi_{timestamp}.json')

        # Write data to a file in the directory
        with open(file_path, 'w') as file:
            json.dump(data, file)
            
        print(f"Data written to {file_path}")
        
    else:
        raise Exception(f"API request failed with status {response.status_code}")

    #Dump data to S3 bucket
    upload_file_to_s3(RAW_BUCKET, PREFIX, file_path)
    print(f"{file_path} uploaded to {RAW_BUCKET}/{PREFIX}")
    
    #Delete data after upload
    os.remove(file_path)
    print(f"{file_path} removed from local")


#Main
load_env()
