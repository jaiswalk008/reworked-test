import pandas as pd
import re
import logging
from states_dict import us_state_to_abbrev

def strip_non_numeric_and_convert_to_float(df, column_name):
    df[column_name] = df[column_name].fillna(0)
    df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9.]', '', str(x))).astype(float)
    return df

def abbrevate_state_names(state):
    try:
        state = str(state)
        if len(state) == 2:
            return state
        else:
            if state.title() in us_state_to_abbrev:
                return us_state_to_abbrev[state.title()]
            else:
                return None
    except Exception as e:
        logging.error("State not found ", e)
        return None

def extract_zip_code(address):
    if pd.isna(address):
        return None
    address_parts = address.split()
    zip_code = None
    if address_parts:
        zip_code_groups = re.search(r'\b\d{5}(?:[-\s]\d{4})?\b', address_parts[-1])
        if zip_code_groups:
            zip_code = zip_code_groups.group(0)
    return zip_code

def create_columns_if_doesnt_exist(df):
    if 'owner_full_name' not in df.columns:
        if (('owner_first_name' in df.columns) and ('owner_last_name' in df.columns)):
            df['owner_full_name'] = df['owner_first_name'].fillna('') + ' ' + df['owner_last_name'].fillna('')
        elif 'owner_mailing_name' in df.columns:
            df['owner_full_name'] = df['owner_mailing_name']
        elif 'owner_first_name' in df.columns:
            df['owner_full_name'] = df['owner_first_name']
    else:
        if (('owner_first_name' in df.columns) and ('owner_last_name' in df.columns)):
            df.loc[df['owner_full_name'].isna(), 'owner_full_name'] = df.loc[df['owner_full_name'].isna(), 'owner_first_name'] + ' ' + df.loc[df['owner_full_name'].isna(), 'owner_last_name']
        elif 'owner_mailing_name' in df.columns:
            df.loc[df['owner_full_name'].isna(), 'owner_full_name'] = df.loc[df['owner_full_name'].isna(), 'owner_mailing_name']
        elif 'owner_first_name' in df.columns:
            df.loc[df['owner_full_name'].isna(), 'owner_full_name'] = df.loc[df['owner_full_name'].isna(), 'owner_first_name']
    
    if 'mail_state_name_short_code' in df.columns:
        df['mail_state_name_short_code'] = df.apply(lambda row: abbrevate_state_names(row['mail_state_name_short_code']), axis=1)
    
    return df

def rename_columns(file_path, column_mapping):
    df = pd.read_csv(file_path, low_memory=False, on_bad_lines='warn')
    df.rename(columns=column_mapping, inplace=True)
    return df