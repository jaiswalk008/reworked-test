import boto3
import os
from botocore.exceptions import NoCredentialsError
from pathlib import Path
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# Initialize the S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')  
)


def upload_s3(key: str, body: str, user_folder: str = 'test') -> dict:
    """
    Upload a file to S3.
    
    :param key: The key or filename for the S3 object.
    :param body: The file path to be uploaded.
    :param user_folder: The folder name where the file should be stored in S3.
    :return: Response from the S3 upload operation.
    """
    bucket_name = os.getenv('S3_BUCKET_NAME', '')
    s3_key = f"{user_folder}/{Path(key).name}"
    
    logging.info(f"Uploading file '{body}' to bucket '{bucket_name}' with key '{s3_key}'.")
    try:
        with open(body, 'rb') as file_body:
            s3_client.upload_fileobj(
                Fileobj=file_body,
                Bucket=bucket_name,
                Key=s3_key
            )
        logging.info("File uploaded successfully.")
        return {"Message": "File uploaded successfully", "Key": s3_key}
    except FileNotFoundError:
        logging.error("The file was not found.")
        raise Exception("The file was not found.")
    except NoCredentialsError:
        logging.error("AWS credentials not available.")
        raise Exception("Credentials not available.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during upload: {str(e)}")
        raise


def download_file_from_s3(filename: str, email: str, expiry: int = 300) -> str:
    """
    Generate a pre-signed URL for downloading a file from S3.
    
    :param filename: The name of the file to be downloaded.
    :param email: The user's email to create a unique key path.
    :param expiry: Expiry time for the pre-signed URL in seconds.
    :return: A pre-signed URL for the S3 object.
    """
    bucket_name = os.getenv('S3_BUCKET_NAME', '')
    s3_key = f"{email}/{filename}"
    
    logging.info(f"Generating pre-signed URL for file '{filename}' in bucket '{bucket_name}' with key '{s3_key}'.")
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': s3_key},
            ExpiresIn=expiry
        )
        logging.info("Pre-signed URL generated successfully.")
        return url
    except Exception as e:
        logging.error(f"Error generating pre-signed URL: {str(e)}")
        raise Exception(f"Error generating pre-signed URL: {str(e)}")


def generate_presigned_s3_url(key: str, user_folder: str = 'test', expire_time: int = 3600) -> str:
    """
    Generate a pre-signed URL for an S3 object.
    
    :param key: The key or filename for the S3 object.
    :param user_folder: The folder name where the file is stored in S3.
    :param expire_time: Expiry time for the pre-signed URL in seconds.
    :return: A pre-signed URL for the S3 object.
    """
    bucket_name = os.getenv('S3_BUCKET_NAME', '')
    s3_key = f"{user_folder}/{Path(key).name}"
    
    logging.info(f"Generating pre-signed URL for key '{s3_key}' in bucket '{bucket_name}'.")
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': s3_key},
            ExpiresIn=expire_time
        )
        logging.info("Pre-signed URL generated successfully.")
        return url
    except Exception as e:
        logging.error(f"Error generating pre-signed URL: {str(e)}")
        raise Exception(f"Error generating pre-signed URL: {str(e)}")
