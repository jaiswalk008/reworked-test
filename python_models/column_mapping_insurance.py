import sys
import pandas as pd
import logging
import json
from column_mapping_helper import create_columns_if_doesnt_exist, rename_columns

REQUIRED_COLUMNS_INSURANCE = [
    "owner_full_name",
    "owner_first_name",
    "owner_last_name",
    "mail_street_address",
    "mail_city",
    "mail_state_name_short_code",
    "mail_zip_code",
    "mail_county"
]

INSURANCE_COLUMN_MAPPINGS = {
    "full name": "owner_full_name",
    "fullname": "owner_full_name",
    "fn": None,  # Explicitly set to None to ignore 'fn'
    "first name": "owner_first_name",
    "firstname": "owner_first_name",
    "last name": "owner_last_name",
    "lastname": "owner_last_name",
    "address 1": "address_1",
    "address1": "address_1",
    "addr1": "address_1",  # Added variations, more can be added similarly
    "add1": "address_1",
    "add-1": "address_1",
    "address 2": "address_2",
    "address2": "address_2",
    "city": "mail_city",
    "state": "mail_state_name_short_code",
    "zipcode": "mail_zip_code",
    "zip code": "mail_zip_code",
    "zip": "mail_zip_code",
    "county name": "mail_county",
    "county": "mail_county"
}

def process_address(df):
    # Combine Address 1 and Address 2 into mail_street_address
    df['mail_street_address'] = df.apply(lambda row: 
        row['address_1'] if pd.notna(row['address_1']) and row['address_1'].upper() != 'NULL' and row['address_1'].strip() != ''
        else (row['address_2'] if 'address_2' in row and pd.notna(row['address_2']) else ''), axis=1)
    
    # Remove Address 1 and Address 2 columns
    df = df.drop(['address_1', 'address_2'], axis=1, errors='ignore')
    
    return df

def process_zipcode(df):
    # Take only the first 5 digits of the zipcode
    df['mail_zip_code'] = df['mail_zip_code'].astype(str).apply(lambda x: x.split('-')[0][:5])
    return df

def handle_name_columns(df):
    """
    Smart handling of name columns based on available data.
    Reconstructs full name from first and last name if both exist and are populated.
    Returns modified DataFrame and list of required columns.
    """
    has_first_name = 'owner_first_name' in df.columns
    has_last_name = 'owner_last_name' in df.columns
    has_full_name = 'owner_full_name' in df.columns

    # If we have both first and last names, we can reconstruct full name
    if has_first_name and has_last_name:
        # Create a mask for rows where both first and last names are populated
        populated_mask = (
            df['owner_first_name'].notna() & 
            df['owner_first_name'].astype(str).str.strip().ne('') &
            df['owner_last_name'].notna() & 
            df['owner_last_name'].astype(str).str.strip().ne('')
        )
        
        # If owner_full_name doesn't exist, create it
        if not has_full_name:
            df['owner_full_name'] = ''
            logging.info("Created 'owner_full_name' column")
        
        # Update owner_full_name only for rows where both first and last names are populated
        df.loc[populated_mask, 'owner_full_name'] = (
            df.loc[populated_mask, 'owner_first_name'].astype(str).str.strip() + 
            ' ' + 
            df.loc[populated_mask, 'owner_last_name'].astype(str).str.strip()
        )
        logging.info("Updated 'owner_full_name' for rows with populated first and last names")

    # Return all required columns since we're keeping owner_full_name
    return df, REQUIRED_COLUMNS_INSURANCE

def map_insurance_columns(file_path, new_name_dict):
    new_file_path = file_path.split('.csv')[0] + '_rwr.csv'
    df = pd.read_csv(file_path, low_memory=False, on_bad_lines='warn')
    
    # Store original column order
    original_columns = df.columns.tolist()
    
    # Initialize a dictionary to track columns to rename
    rename_dict = {}
    
    # If caller of the script sends the column mapping, just use that
    source = ""
    if new_name_dict:
        logging.info("column_mapping - using custom mapping", new_name_dict)
        for old_name, new_name in new_name_dict.items():
            if old_name.lower().strip() == 'fn':
                # Ignore 'FN' for full name
                continue
            if old_name in df.columns:
                rename_dict[old_name] = new_name
        source = "custom"
    else:
        # This is where we're renaming the columns from the original file
        for column_name in df.columns:
            lower_column = column_name.lower().strip()
            if lower_column in INSURANCE_COLUMN_MAPPINGS:
                mapped_name = INSURANCE_COLUMN_MAPPINGS[lower_column]
                if mapped_name:  # Only rename if mapped_name is not None
                    rename_dict[column_name] = mapped_name

    # Perform renaming
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True)
    
    # Remove duplicate columns
    df = df.loc[:,~df.columns.duplicated()].copy()
    
    # If 'FN' was present, drop it
    if 'FN' in df.columns:
        df.drop('FN', axis=1, inplace=True)
    
    # Process address and zipcode
    df = process_address(df)
    df = process_zipcode(df)
    
    # Handle name columns and get updated required columns
    df, required_columns = handle_name_columns(df)
    
    # Create missing columns
    for required_column in required_columns:
        if required_column not in df.columns:
            df[required_column] = ''
    
    # Check if all required columns are present
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        error_message = f"Required columns don't exist for {source} file, columns missing are: {', '.join(missing_columns)}"
        if source == "custom":
            error_message += f", column mapping used: {str(new_name_dict)}"
        raise Exception(error_message)

    # Reorder columns to match original order and include new required columns
    new_columns = []
    for col in original_columns:
        lower_col = col.lower().strip()
        if lower_col in INSURANCE_COLUMN_MAPPINGS:
            new_col = INSURANCE_COLUMN_MAPPINGS[lower_col]
            if new_col and new_col in df.columns:
                new_columns.append(new_col)
        elif col in df.columns:
            new_columns.append(col)

    # Add any missing required columns
    for col in required_columns:
        if col not in new_columns:
            new_columns.append(col)

    df = df[new_columns]

    return new_file_path, df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    new_name_dict = {}
    file_path = ""

    for i, arg in enumerate(sys.argv):
        if arg == '--file_path':
            file_path = sys.argv[i+1]
        if arg == "--custom_mapping":
            new_name_dict = json.loads(sys.argv[i+1])

    try:
        new_file_path, df = map_insurance_columns(file_path, new_name_dict)
        df.to_csv(new_file_path, index=False)
        logging.info({"success": "True", "OriginalFilePath": file_path, "NewFilePath": new_file_path})
    except Exception as e:
        exc = str(e).replace('"', '').replace("'", '')
        logging.error({"success": "False", "OriginalFilePath": file_path, "NewFilePath": "", "error": "column_mapping_failure", "error_details": exc})