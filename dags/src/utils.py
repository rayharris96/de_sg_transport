import os
import boto3
import logging
import pandas as pd
import io



def load_env():
    """Load variables from .env file into environment variables.

    :param filepath: path to the .env file
    """
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../.env')
    with open(env_path, 'r') as f:
        for line in f:
            if line.startswith('#') or not line.strip():
                continue
            key, value = line.strip().split('=', 1)
            os.environ[key] = value

def create_s3_client():
    access_key_id = os.getenv("AWS_ACCESS_KEY")
    secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if access_key_id is None or secret_access_key is None:
        raise Exception('Missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY in environment variables')
        
    s3_client = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
    return s3_client

def upload_file_to_s3(bucket_name, prefix, file_path):
    """
    Upload a file to an S3 bucket
    Example: upload_file_to_s3(raw-kungfu-challenger, bus_stop, 202212313.json)
    """
    # Automatically use the base filename as the object name
    object_name = os.path.join(prefix, os.path.basename(file_path))

    s3_client = create_s3_client()
    try:
        response = s3_client.upload_file(file_path, bucket_name, object_name)
        logging.info(f"{file_path} uploaded to {bucket_name}")

    except Exception as e:
        print(e)
        return False
    return True

def download_latest_file_from_s3(bucket_name, prefix):
    """Download the latest file from an S3 bucket"""

    s3_client = create_s3_client()

    #Create default download data folder
    data_dir = 'download_data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    try:
        # List all objects within the specified bucket and prefix
        files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        # Check if there are any files
        if files['KeyCount'] > 0:
            # Sort the files by last modified date/time
            sorted_files = sorted(files['Contents'], key=lambda x: x['LastModified'], reverse=True)
            
            # Get the name of the most recently modified file
            latest_file = sorted_files[0]['Key']
            
            # If no file path is given, save the file in the 'download_data' folder with its original name
            file_path = os.path.join(data_dir, os.path.basename(latest_file))

            # Download the most recently modified file
            s3_client.download_file(bucket_name, latest_file, file_path)
            logging.info(f"File {latest_file} downloaded to {file_path}")
        else:
            print('No files found in the specified bucket and prefix.')
            return False
    except Exception as e:
        print(e)
        return False

    return file_path

def download_all_files_from_s3(bucket_name, prefix):
    """Download all files from an S3 bucket"""

    s3_client = create_s3_client()

    #Create default download data folder
    data_dir = 'download_data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    try:
        # List all objects within the specified bucket and prefix
        files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        # Check if there are any files
        if files['KeyCount'] > 0:
            # Download each file
            for file in files['Contents']:
                file_name = file['Key']

                # If no file path is given, save the file in the 'download_data' folder with its original name
                file_path = os.path.join('download_data', os.path.basename(file_name))

                # Download the file
                s3_client.download_file(bucket_name, file_name, file_path)
        else:
            print('No files found in the specified bucket and prefix.')
            return False
    except Exception as e:
        print(e)
        return False

    return file_path


def read_csvfile_from_s3(bucket_name, object_key):
    s3_client = create_s3_client()

    try:
        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

        # Read the file data
        file_data = response['Body'].read().decode('utf-8')

        # Parse the CSV data into a pandas DataFrame
        df = pd.read_csv(io.StringIO(file_data), header = 1)

        return df

    except Exception as e:
        print("Error reading the CSV file from S3:", str(e))
        return None