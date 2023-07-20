from utils import load_env, download_latest_file_from_s3, upload_file_to_s3
import os

def transform_lta_bus():
    RAW_BUCKET = 'raw-kungfu-challenge'
    PREFIX = 'bus_stop'

    #Download raw bus data from S3
    download_latest_file_from_s3(RAW_BUCKET, PREFIX)

    return 

#Main
load_env()
transform_lta_bus()