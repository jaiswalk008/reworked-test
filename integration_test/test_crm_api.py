import requests
import time
import logging
from io import StringIO
import pandas as pd

# Function to check the file status
def check_file_status(file_identifier, token, email):
    status_url = f'{base_url}/file-status'
    headers = {
        'Authorization': f'{token}',
        'Content-Type': 'application/json'
    }
    payload = {
        "file_upload_identifier": file_identifier,
        "email": email
    }
    response = requests.post(status_url, json=payload, headers=headers)
    
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(f"Error checking file status: {err}")
        return None
    
    return response.json()

# Base URL of the API service
# test_email_account = 'harshitkyal@gmail.com'
test_email_account = 'harshitkyal+test01@gmail.com'
# base_url = 'https://f7a3-2405-201-200c-898-b007-3531-19e6-2d60.ngrok-free.app/api'
base_url = 'https://devapp.reworked.ai/bnd/api'
api_key = 'f30b-5a2c-414f-5025-5538-8427-ab6a-70d2-35c8-3361-cbf2-957b-5ac5-96e7-b9bb-2978'

# Set the initial value of the success flag
success = True
error_message = ''
try:
    # First API request to generate token
    generate_token_url = f'{base_url}/generate-token'
    generate_token_payload = {
        "api_key": api_key,
        "email": test_email_account
    }

    print("Step 1: Generating token...")
    generate_token_response = requests.post(generate_token_url, json=generate_token_payload)
    generate_token_response.raise_for_status()

    # Extracting token from the response
    token = generate_token_response.json().get('data', {}).get('token')
    print("Token generated successfully!")

    # Second API request to process leads using the obtained token
    process_leads_url = f'{base_url}/process-leads'
    process_leads_payload = {
        "file_url": "https://drive.google.com/uc?export=download&id=1MRyDLDBibbQ7qrKs0mFWAGbFQ-wWEnU9",
        "email": test_email_account,
        "overwrite": True
    }

    headers = {
        'Authorization': f'{token}',
        'Content-Type': 'application/json'
    }

    print("Step 2: Processing leads...")
    process_leads_response = requests.post(process_leads_url, json=process_leads_payload, headers=headers)
    process_leads_response.raise_for_status()

    # Extract token and file identifier from the process leads response
    token_from_process_leads = process_leads_response.json().get('data', {}).get('token')
    file_identifier = process_leads_response.json().get('data', {}).get('file_upload_identifier')
    print("Leads processed successfully!")

    # Print the response from the second API call
    print("API Response:", process_leads_response.json())

    # Check file status in a loop
    max_attempts = 30
    wait_time_seconds = 10

    for attempt in range(1, max_attempts + 1):
        print(f"Attempt {attempt}: Checking file status...")
        status_response = check_file_status(file_identifier, token_from_process_leads, test_email_account)
        if status_response is None:
            # Log the error and break the loop
            logging.error({
                "success": False,
                "error": "Error checking file status",
                "error_details": "Failed to establish a connection with the server"
            })
            print("Error checking file status. Exiting.")
            success = False
            error_message = "Failed to establish a connection with the server"
            break

        status = status_response.get('data', {}).get('status')

        if status == "PROCESSED":
            print("File Processed Successfully!")
            processed_file_url = status_response.get('data', {}).get('processed_file_url')
            print("Processed file url!")
            print(f"Processed file url, {processed_file_url}")
            response = requests.get(processed_file_url)
            print(f"Status code, {response.status_code}")
            # Download the processed file
            
            if response.status_code == 200:
                # Check if the file contains the "BETTY SCORE" column and value
                file_content = response.text
                csv_data = StringIO(file_content)
                df = pd.read_csv(csv_data)
                print(f"Column in file, {df.columns}")
                if 'BETTY SCORE' in df.columns and not df[df['BETTY SCORE'].notna()].empty:
                    print("File contains 'BETTY SCORE' column with values.")
                    logging.info({
                        "success": True,
                        "processed_file_url": processed_file_url
                    })
                else:
                    print("File does not contain 'BETTY SCORE' column with values.")
                    logging.error({
                        "success": False,
                        "error_details": 'File doesnt contain BETTY SCORE Column'
                    })
                    success = False
                    error_message = 'File doesnt contain BETTY SCORE Column'
            break
        elif status == "ERROR":
            print("File Processing Error!")
            logging.error({
                "success": False,
                "error_details": status_response.get('data', {}).get('error_detail')
            })
            success = False
            error_message = status_response.get('data', {}).get('error_detail')
            break
        else:
            print(f"File status is {status}. Waiting for {wait_time_seconds} seconds.")
            time.sleep(wait_time_seconds)
    else:
        print(f"Maximum attempts ({max_attempts}) reached. File status not yet 'PROCESSED' or 'ERROR'.")
        logging.error({
            "success": False,
            "error": "Maximum attempts reached",
            "error_details": "File status not yet 'PROCESSED' or 'ERROR'."
        })
        error_message = "File status not yet 'PROCESSED' or 'ERROR'."
        success = False

except requests.exceptions.RequestException as e:
    logging.error({
        "success": False,
        "error": "Request Exception",
        "error_details": str(e)
    })
    print(f"An error occurred: {e}")
    error_message = str(e)
    success = False

if not logging.getLogger().handlers:
    # Configure logging if not already configured
    logging.basicConfig(level=logging.INFO)

if not success:
    raise Exception(f"Lead processing failed, Error: {error_message}. Check the logs for details.", )
