from .utils import load_env, download_latest_file_from_s3, upload_file_to_s3
import pandas as pd
import json
import os


def transform_lta_bus():
    RAW_BUCKET = 'raw-kungfu-challenge'
    PREFIX = 'bus_stop'

    #Download raw bus data from S3
    file_path = download_latest_file_from_s3(RAW_BUCKET, PREFIX)
    print(f"File saved to {file_path}")
    f = open(file_path)
    data = json.load(f)
    df = pd.json_normalize(data['Services'])
    # df = df[['ServiceNo', 'Operator', 'EstimatedArrival', 'Load', 'Feature']]
    df.to_csv(os.path.join('transformed_data','transformed.csv'))


#Main
load_env()
