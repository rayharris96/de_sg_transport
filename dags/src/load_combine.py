import requests
import os
import glob
import pandas as pd
import json
from datetime import datetime
from .utils import load_env, upload_file_to_s3, download_all_files_from_s3, download_latest_file_from_s3

def combine_bus_timing():
    """
    This is to combine bus timing and load into presentation bucket
    """
    PREFIX = 'bus_stop'
    TRANSFORM_BUCKET = 'staging-kungfu-challenge'
    PRESENTATION_BUCKET = 'presentation-kungfu-challenge'

    #Download transformed bus data from S3
    dir_path = download_all_files_from_s3(TRANSFORM_BUCKET, PREFIX, 'bus_stop')
    print(f"File saved to {dir_path}")
    complete_file_path = os.path.join(dir_path, 'complete_bus_data.csv')

    # Use glob to match the pattern 'csv'
    files = glob.glob('download_all_data/bus_stop/*.csv')
    # Create an empty list to hold dataframes
    df_list = pd.DataFrame(None)
    # Loop through list of files and read each one into a dataframe
    for file in files:
        try:
            df_list = df_list + pd.read_csv(file)
        except:
            pass
        os.remove(file)
        
    # Concatenate all dataframes in the list
    combined_df = df_list
    combined_df.to_csv(complete_file_path, index=False)

    upload_file_to_s3(PRESENTATION_BUCKET, PREFIX, complete_file_path)

    print(f"File uploaded to S3 {PRESENTATION_BUCKET}/{PREFIX}/")


load_env()
