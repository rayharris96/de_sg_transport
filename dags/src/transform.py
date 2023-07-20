from .utils import load_env, download_latest_file_from_s3, upload_file_to_s3
import pandas as pd
import json
import os

TRANSFORMED_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../transformed_data')
if not os.path.exists(TRANSFORMED_FOLDER):
    os.makedirs(TRANSFORMED_FOLDER)

def transform_lta_bus():
    RAW_BUCKET = 'raw-kungfu-challenge'
    PREFIX = 'bus_stop'

    #Download raw bus data from S3
    file_path = download_latest_file_from_s3(RAW_BUCKET, PREFIX)
    f = open(file_path)
    data = json.load(f)
    df = pd.json_normalize(data['Services'])
    # df = df[['ServiceNo', 'Operator', 'EstimatedArrival', 'Load', 'Feature']]
    df.to_csv(os.path.join(TRANSFORMED_FOLDER,'transformed.csv'))

    return 

#Main
load_env()
transform_lta_bus()