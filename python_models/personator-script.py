import pandas as pd
from python_models.lead_sorting.utils import get_person_details_from_email, read_file
import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(message)s')

try:
    logging.info("1. Starting column_mapping code.... \n")
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', '-f', type=str, required=True, help='Please add a CSV filename, eg - filename.csv')

    args = parser.parse_args()

    file_path = args.file_path
    base_file_path = file_path.rsplit('.', 1)[0]

    df = read_file(file_path)
    num_records = len(df)
    print("num_records", num_records)
    # Define a function to apply to each row of the DataFrame
    def update_row(row):
        email = row['email']
        if email != None:
          details = get_person_details_from_email(email)
          # return details
          # print("details", details)
          # # Update DataFrame based on 'details'
          # # Create new columns for each key in 'details' if they don't exist
          try:
            for key, value in details.items():
                # print("value", value)
                if key not in df.columns:
                    df[key] = ''  # Create a new column with the key
                df.loc[df.index[df['email'] == email], key] = value  # Update the DataFrame with the value
          except Exception as e:
            print(f"Error: {e}")
            # return None

    # Apply the function to each row
    df.apply(lambda row: update_row(row), axis=1)

    # Save the updated DataFrame
    df.to_csv(f'{base_file_path}_data_appended.csv', index=False)
    logging.info({"success":"True","OriginalFilePath": file_path, "NewFilePath": "_rwr.csv"})

except Exception as e:
    exc = str(e).replace('"','')
    exc = str(exc).replace("'",'')
    pass
    logging.error({"success":"False","OriginalFilePath": file_path, "error": "personator-script", "error_details": exc})
