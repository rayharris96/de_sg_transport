import os
import boto3
import logging
import git


def get_git_root(path):

    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return git_root


def load_env():
    """Load variables from .env file into environment variables.

    :param filepath: path to the .env file
    """
    git_root = get_git_root(os.getcwd())

    env_path = os.path.join(git_root, '.env')
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

def download_file_from_s3(bucket_name, prefix, object_name, file_path=None):
    """Download a file from an S3 bucket"""

    if file_path is None:
        # If no file path is given, use the object name as the file name
        file_path = object_name

    object_name = os.path.join(prefix, object_name)

    s3_client = create_s3_client()
    try:
        s3_client.download_file(bucket_name, object_name, file_path)
    except Exception as e:
        print(e)
        return False
    return True
