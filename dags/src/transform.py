from .utils import load_env, download_latest_file_from_s3, upload_file_to_s3,read_csvfile_from_s3
import pandas as pd
import json
import os
from datetime import datetime, timedelta, timezone

def transform_lta_bus():
    RAW_BUCKET = 'raw-kungfu-challenge'
    PREFIX = 'bus_stop'
    TRANSFORM_BUCKET = 'staging-kungfu-challenge'

    #Download raw bus data from S3
    file_path = download_latest_file_from_s3(RAW_BUCKET, PREFIX)
    print(f"File saved to {file_path}")
    f = open(file_path)
    data = json.load(f)
    df = pd.json_normalize(data['Services'])
    df = df[['ServiceNo', 'NextBus.EstimatedArrival', 'NextBus.Latitude', 'NextBus.Longitude']]
    df = df.rename(columns={
        'ServiceNo': "bus_no",
        'NextBus.EstimatedArrival': 'arrival_time',
        'NextBus.Latitude': 'latitude',
        'NextBus.Longitude': 'longitude',
    })

    filename = os.path.basename(file_path)
    filename = filename.rsplit('.', 1)[0]
    # Extract the date and time string from the filename
    datetime_str = filename.split('_')[-2] + '_' + filename.split('_')[-1]
    # Convert the string to a datetime object
    extraction_time = datetime.strptime(datetime_str, '%Y%m%d_%H%M%S').replace(tzinfo=timezone.utc)

    df['arrival_mins'] = df['arrival_time'].apply(lambda x: abs(round((datetime.strptime(x, '%Y-%m-%dT%H:%M:%S%z') - extraction_time).total_seconds()/60,2)))
    df['execution_time'] = extraction_time.replace(tzinfo=timezone(timedelta(hours=8)))

    temp_data_file_path = os.path.join('transformed_data',f'transformed_{datetime_str}.csv')
    df.to_csv(temp_data_file_path, index=False)
    upload_file_to_s3(TRANSFORM_BUCKET, PREFIX, temp_data_file_path)
    print(f"File uploaded to S3 {TRANSFORM_BUCKET}/{PREFIX}/{filename}")
    os.remove(temp_data_file_path)
    print(f"{temp_data_file_path} removed from local")


def transform_lta_erp_rate():

    DEV = False
    RAW_BUCKET = 'raw-kungfu-challenge'
    PRESENTATION_BUCKET = 'presentation-kungfu-challenge'
    PREFIX_ERP = 'erp_rate'

    erp_zone_object_key = f'{PREFIX_ERP}/gantry_zone/erp_zone_location.csv'

    if DEV == True:
        erp_zone_file_path = os.path.join('../data','erp_zone_location.csv')
        zone_data = pd.read_csv(erp_zone_file_path,header = 1)
        file_path = os.path.join('./data','data_20230720_235013.json')

    else:
        zone_data = read_csvfile_from_s3(RAW_BUCKET,erp_zone_object_key)
        file_path = download_latest_file_from_s3(RAW_BUCKET, PREFIX_ERP)



    raw_df = pd.read_json(file_path)
    erp_df = pd.json_normalize(raw_df['value'])
    combined_df = erp_df.merge(zone_data,left_on= 'ZoneID',right_on = 'Zone ID')

    temp_data_file_path = os.path.join('./data','erp_gantry_data.csv')
    erp_presentation = combined_df.to_csv(temp_data_file_path,index = False)

    upload_file_to_s3(PRESENTATION_BUCKET, PREFIX_ERP, temp_data_file_path)

    os.remove(temp_data_file_path)
    print(f"{temp_data_file_path} removed from local")

#Main
load_env()