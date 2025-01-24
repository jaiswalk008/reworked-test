_llm_pipe = None
_llm_tokenizer = None

import os
os.environ['KMP_DUPLICATE_LIB_OK']='TRUE'

import pandas as pd
import logging
import time 
import numpy as np
import re
import requests
import datetime
import pymongo
from dotenv import load_dotenv
import usaddress
try:
    from sklearn.decomposition import PCA
except ImportError:
    PCA = None
    logging.warning("scikit-learn is not installed or the version if different compared original code vs insurance changes. PCA functionality will be disabled.")

import pickle 
import pathlib
from states_dict import us_state_to_abbrev
import usaddress
import numpy as np
from parsernaam import ParseNames
#from langchain_community.llms import Ollama
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from huggingface_hub import login
import warnings
import torch
from tqdm import tqdm
from huggingface_hub import login
# Suppress warnings and configure logging
warnings.filterwarnings("ignore")

# Configure logging for the utils module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    # Don't create handlers here - let the root logger handle it
    logger.propagate = True  # This is important! Allows logs to propagate up

import pytz
load_dotenv()
dbclient = pymongo.MongoClient(os.getenv('MONGO_URL'))
mydb= dbclient['myFirstDatabase']
demographicDataCollection = mydb["DemographicData"]
from flair.models import SequenceTagger
from flair.data import Sentence

# Removed global tagger initialization
# tagger = SequenceTagger.load('ner')  # Remove this line

# Add a function to lazy load the tagger when needed
def get_tagger():
    global tagger
    if not hasattr(get_tagger, 'tagger'):
        get_tagger.tagger = SequenceTagger.load('ner')
    return get_tagger.tagger

""" Converts full address to address parts """
def processAddress(fullAddress):
    if not fullAddress:
        return None, None, None, None
    parsed = usaddress.parse(fullAddress)
    street_address = ""
    zip = None
    state = None
    city = None
    possibleMetaTagsForLine1 = ['AddressNumber', 'StreetNamePreDirectional', 'StreetName', 'StreetNamePostType', 'USPSBoxID', 'USPSBoxType', 'USPSBoxType', 'StreetNamePreModifier', 'StreetNamePreType', 'OccupancyIdentifier', 'OccupancyIdentifier']
    
    # Additional variable to concatenate PlaceName values
    placeName = ""
    
    for i in parsed:
        if i[1] in possibleMetaTagsForLine1:
            street_address += " " + i[0]
        if i[1] == 'ZipCode':
            zip = i[0]
            if '-' in zip:
                zip = zip.split('-')[0]  # Extract main ZIP code without the additional characters
        if i[1] == 'StateName':
            state = i[0]
        if i[1] == 'PlaceName':
            # Concatenate PlaceName values
            if city:
                city += " " + i[0]
            else:
                city = i[0]
    
    if street_address:
        street_address = street_address.lstrip()
    if city:
        city = city.replace(',', '')
    
    return street_address, city, state, zip 


COLUMN_REPLACE_DICT = {
    # zipcode
    'zip': 'zipcode',
    'zip_code': 'zipcode',
    'billing_zip_postal_code': 'zipcode',
    'postal_code': 'zipcode',
    'postalcode': 'zipcode',
    'mail_zip_code':'zipcode',
    'mailing_zip_postal_code':'zipcode',
    'property_zip_code': 'zipcode',
    'property_zipcode': 'zipcode',
    # email_address
    'e-mail': 'email_address',
    'email_address': 'email_address',
    'buyer_address': 'email_address',
    'buyer_email': 'email_address',
    # full_name
    'name': 'full_name',
    'owner_full_name': 'full_name',
    'buyer_full_name': 'full_name',
    'buyer:full_name': 'full_name',
    'owner_full_name': 'full_name',
    # first_name
    'buyer_first_name': 'first_name',
    'buyer_firstname': 'first_name',
    'firstname':'first_name',
    'owner_first_name': 'first_name',
    # last_name
    'buyer_last_name': 'last_name',
    'buyer_lastname': 'last_name',
    'lastname': 'last_name',
    'owner_last_name': 'last_name',
    # state
    'billing_state/province': 'state',
    'billing_state':'state',
    'property_state_name_short_code':'state',
    'mail_state_name_short_code':'state',
    # full_address
    'billing_address':'full_address',
    'mail_full_address':'full_address',
    'property_address_full':'full_address',
    'billing_address':'full_address',
    # first_address
    'address_1': 'street_address',
    # 'street_address': 'street_address',
    'street_address_1': 'street_address',
    # second_address
    'address_2': 'second_Address',
    'street_address_2': 'street_address',
    # city
    'billing_city':'city'
}

RELEVANT_COLUMNS = [
    'city', 'zipcode', 'email_address', 'full_name', 
    'first_name', 'last_name', 'state', 'full_address'
]

REQUIRED_COLUMNS  = [
    'full_name', 'zipcode', 'full_address'
]

REQUIRED_COLUMNS_V_11_2  = [
    'full_name', 'full_address'
]

BUSINESS_NAMES = ['LLC', 'LTD', 'INC', 'CHURCH',  'ASSET', 'INC', 'PROPERTIES' , 'SCHOOL'
                      'VENTURES', 'ESTATE', 'EST', 'MORTGAGE', 'EQUITY', 'HOLDINGS', 'COMPANY', 'CATHOLIC', 'BAPTIST', 'BUILDERS', 'ASSOCIATION', 'NEIGHBORHOOD']

TRUST_NAMES = ['TRUST', 'TRUSTEE', 'TRUSTEES','FAMILY', 'REVOCABLE', 'IRREVOCABLE', 'LIVING', 'RESIDUARY', 'REVOC', 'PROTECTION', 'TRST', 'MARITAL', 'GRANDCHILD', 
               'DEDUCTION', 'BYPASS', 'INVESTMENT', 'TR', 'GST', 'MANAGEMENT', 'SUPPLEMENTAL', 'SCHOLARSHIP', 'EQUITY', 'AGREEMENT', 'ESTATE', 'TRUSTS', 'DESCENDENTS', 'PROPERTY', 'EST']

# Strip a name of single-letter initials, middle initials, and salutations.
def strip_name(name):
    """
    Strip a name of single-letter initials, middle initials, and salutations.

    Args:
    name (str): The input name to be stripped.

    Returns:
    str: The stripped name.
    """
    # List of common salutations to remove
    salutations = ['Mr.', 'Ms.', 'Mrs.', 'Dr.', 'Miss', 'Sir', 'Madam', 'Sir/Madam', 'Jr', 'Sr', 'Jr.', 'Sr.']

    # Split the name into parts
    parts = name.split()

    # Initialize a list to hold the filtered parts
    filtered_parts = []

    # Iterate through parts and filter out single-letter initials, middle initials, and salutations
    for part in parts:
        if len(part) > 1 or part.isdigit():  # Keep parts that are not single-letter initials or numbers
            if part not in salutations:  # Keep parts that are not salutations
                filtered_parts.append(part)

    # Join the filtered parts to form the stripped name
    stripped_name = ' '.join(filtered_parts)

    return stripped_name

# Function that swaps owner 1 and owner 2 names if needed
def swap_names_if_needed(o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n):
    # If both owners' first and last names are present, return them
    if o_1_f_n is not None and o_1_l_n is not None:
        return o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n
    if o_1_f_n is not None and o_1_l_n is None and o_2_l_n is not None:
        return o_1_f_n, o_2_l_n, o_2_f_n, o_2_l_n
    elif o_2_f_n is not None and o_2_l_n is not None:
        return o_2_f_n, o_2_l_n, o_1_f_n, o_1_l_n
    
    # Handle cases where one owner's first name or last name is present and the other is not
    if o_1_f_n is None and o_1_l_n is not None:
        return o_2_f_n, o_1_l_n, None, None
    elif o_2_f_n is None and o_2_l_n is not None:
        return o_1_f_n, o_2_l_n, None, None
    
    # If none of the conditions above are met, return the input values unchanged
    return o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n
    

# Function that determines if the owner is a business entity:
'''def get_is_business_or_trust(full_name):
    is_business = False
    is_trust = False

    full_name = full_name.title()

    if pd.isna(full_name) or full_name == '':
        return False, False
    names = ''
    names = (str(full_name).upper()).split(' ')
    if (np.in1d(BUSINESS_NAMES, names)).any():
        is_business = True
    if (np.in1d(TRUST_NAMES, names)).any():
        is_trust = True
    if (is_business):
        return is_business, is_trust
    
    # Check Spacy if rule engine check did not yield business
    stripped_name = full_name.replace('&', '')
    stripped_name = stripped_name.replace('-', '')
    stripped_name = stripped_name.replace(',', ' ')
    stripped_name = stripped_name.replace(' and ', '')
    doc = nlp(stripped_name)
    try:
        spacy_org = doc.ents[0].label_
    except:
        spacy_org = None
    if spacy_org not in ['PERSON', None]:
        is_business = True 
    
    return is_business, is_trust'''


def get_is_business_or_trust_rule_engine(full_name):
    """
    Check if a name is a business/trust using rule-based approach.
    Returns tuple of (is_business, is_trust)
    """
    logger.info(f"Checking business/trust with rules for name: {full_name}")
    is_business = False
    is_trust = False

    if pd.isna(full_name) or full_name == '':
        return False, False

    names = (str(full_name).upper()).split(' ')
    
    # Check for business indicators
    if (np.in1d(BUSINESS_NAMES, names)).any():
        logger.info(f"Rule-based business match found for: {full_name}")
        is_business = True
        
    # Check for trust indicators
    if (np.in1d(TRUST_NAMES, names)).any():
        logger.info(f"Rule-based trust match found for: {full_name}")
        is_trust = True

    return is_business, is_trust

def get_is_business_or_trust_flair(full_name):
    """
    Check if a name is a business/trust using Flair NER.
    Returns tuple of (is_business, is_trust, needs_llm)
    """
    logger.info(f"Checking business/trust with Flair for name: {full_name}")
    
    # Clean and normalize the name for better NER processing
    # Convert to title case for better person detection
    stripped_name = full_name.replace(',', ' ')  # Replace comma with space
    name_parts = stripped_name.split()
    normalized_name = ' '.join(part.title() for part in name_parts)  # Convert to title case
    
    # Handle common name suffixes
    suffixes = {'Jr', 'Sr', 'II', 'III', 'IV', 'V'}
    for suffix in suffixes:
        normalized_name = normalized_name.replace(f" {suffix.upper()}", f" {suffix}")
        normalized_name = normalized_name.replace(f" {suffix.title()}", f" {suffix}")
    
    sentence = Sentence(normalized_name)
    tagger = get_tagger()
    tagger.predict(sentence)
    
    # Get all entity spans and their labels
    spans = sentence.get_spans('ner')
    labels = [entity.get_label("ner").value for entity in spans]
    
    # If we detect a person entity, it's not a business
    if 'PER' in labels:
        return False, False, False
        
    # If no clear person pattern and no PER label, let LLM verify
    return False, False, True


# Use AI to parse a name into first and last
def parse_name(full_name):
    """
    Parses a full name into first and last names, and identifies the name order type.

    Args:
        full_name (str): The full name to parse.

    Returns:
        tuple: (first_name, last_name, type)
            - first_name (str or None): The parsed first name.
            - last_name (str or None): The parsed last name.
            - type (str): The detected name order type ('first_last', 'last_first', 'first', 'last').
    """
    full_name = full_name.title()
    full_name = strip_name(full_name)
    if len(full_name) <= 1:
        return None, None, 'unknown'
    if ',' in full_name:
        name_parts = full_name.split(',')
        name_parts = [s.strip() for s in name_parts]
        full_name = ' '.join(name_parts)

    df = pd.DataFrame({'name': [full_name]})
    df = ParseNames.parse(df)
    parsed_name = df['parsed_name'][0]
    type = parsed_name.get('type', 'unknown')
    probability = parsed_name.get('prob', 0)
    # print('PARSED NAME', parsed_name, type, probability)
    # Remove middle name if present
    if ' ' in full_name:
        name_parts = full_name.split()
        if len(name_parts) == 3:
            if type == 'last_first':
                full_name = f"{name_parts[0]} {name_parts[1]}"
            elif type == 'first_last':
                full_name = f"{name_parts[0]} {name_parts[2]}"
        full_name = " ".join(full_name.split())

    # Adjust type based on probability
    if probability < 0.56:
        if type == 'last':
            type = 'first'
        elif type == 'last_first':
            type = 'first_last'

    # Assign first and last names based on type
    if type == 'first':
        return full_name, None, type
    elif type == 'last':
        return None, full_name, type
    elif type == 'last_first':
        parts = full_name.split()
        return parts[-1], parts[0], type
    elif type == 'first_last':
        parts = full_name.split()
        return parts[0], parts[-1], type
    else:
        return None, None, 'unknown'

# Function that takes an owner_full_name or owner_mailing_name and returns o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n (if present)
def clean_and_split_owner_names(full_name, split_names_present, owner_first_name, owner_last_name):
    """
    First pass of name parsing using rule-based approach. Defers LLM processing for batch handling.
    
    Args:
        full_name (str): The full name of the owner.
        split_names_present (bool): Flag indicating if first and last names are provided separately.
        owner_first_name (str or None): The owner's first name if provided.
        owner_last_name (str or None): The owner's last name if provided.
    
    Returns:
        tuple: (o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, type, needs_llm)
    """
    logger.info(f"Processing name: {full_name}")
    
    # Initialize return values
    o_1_f_n = None 
    o_2_f_n = None
    o_1_l_n = None
    o_2_l_n = None
    o_b_n = None
    name_type = 'unknown'
    needs_llm = False
    
    if pd.isna(full_name) or full_name == '':
        logger.debug("Empty or null name provided")
        return o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, name_type, needs_llm
    
    # Handle repeated '&' symbols and clean the name
    full_name = re.sub(r'&\s*&', '&', full_name.strip())
    full_name, suffix = extract_name_suffix(full_name)
    
    # Handle provided first/last names early check
    if split_names_present and (owner_first_name is not None) and (isinstance(owner_first_name,str)) and (owner_last_name is not None) and (isinstance(owner_last_name,str)):
        logger.debug(f"Using provided first/last name: {owner_first_name} {owner_last_name}")
        combined_name = f"{owner_first_name} {owner_last_name}"
        
        # Check with rule engine first
        is_business, is_trust = get_is_business_or_trust_rule_engine(combined_name)
        
        if is_business:
            logger.info(f"Rule engine identified business from split names: {combined_name}")
            return None, None, None, None, combined_name, 'business', False
        if is_trust:
            logger.debug("Detected as trust name from split names")
            o_b_n = combined_name
            individual_names = combined_name.upper().split()
            individual_names = [name for name in individual_names if name not in (TRUST_NAMES + BUSINESS_NAMES)]
            if individual_names:
                # Remove trust and business names from owner_first_name and owner_last_name
                cleaned_owner_first_name = ' '.join(
                    [name for name in owner_first_name.split() if name.upper() not in (TRUST_NAMES + BUSINESS_NAMES)]
                )
                cleaned_owner_last_name = ' '.join(
                    [name for name in owner_last_name.split() if name.upper() not in (TRUST_NAMES + BUSINESS_NAMES)]
                )
                
                o_1_f_n = cleaned_owner_first_name if cleaned_owner_first_name else None
                o_1_l_n = cleaned_owner_last_name if cleaned_owner_last_name else None
            return o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, 'first_last', False
        
        # If not business or trust, proceed with original first/last assignment
        o_1_f_n = owner_first_name
        o_1_l_n = owner_last_name
        return o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, 'first_last', False

    # First check: Rule-based business/trust detection
    is_business, is_trust = get_is_business_or_trust_rule_engine(full_name)
    
    if is_business:
        logger.info(f"Rule engine identified business: {full_name}")
        return None, None, None, None, full_name, 'business', False
        
    if is_trust:
        logger.info(f"Rule engine identified trust: {full_name}")
        o_b_n = full_name
        individual_names = [name for name in full_name.upper().split() 
                          if name not in (TRUST_NAMES + BUSINESS_NAMES)]
        full_name = ' '.join(individual_names)
    
    # Second check: Flair-based detection (only if not already identified as business/trust)
    if not (is_business or is_trust):
        _, _, needs_llm = get_is_business_or_trust_flair(full_name)
        if needs_llm:
            logger.info(f"Marking for LLM verification: {full_name}")
            return None, None, None, None, full_name, 'business', True

    # Use parse_person_name_simple for name parsing
    o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, name_type, needs_llm = parse_person_name_simple(full_name)
    
    # Add suffix back to business name if it was a trust
    if is_trust and suffix:
        o_b_n = f"{o_b_n} {suffix}"
        
    return o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, name_type, needs_llm


def process_names_with_llm(df):
    """
    Process all names in a DataFrame, handling business names in batches with LLM.
    """
    logger.info("Entering process_names_with_llm")
    logger.info(f"Number of rows needing LLM: {df['needs_llm'].sum()}")
    
    # Initialize LLM
    pipe, tokenizer = get_llm()
    
    # Process only rows that need LLM
    for idx, row in df[df['needs_llm']].iterrows():
        name = row['owner_full_name']
        logger.info(f"Processing with LLM: {name}")
        
        is_business = verify_business_name(pipe, tokenizer, name)
        
        if is_business:
            df.at[idx, 'temp_business_name'] = name
            df.at[idx, 'temp_name_type'] = 'business'
            df.at[idx, 'temp_owner1_first_name'] = None
            df.at[idx, 'temp_owner1_last_name'] = None
            df.at[idx, 'temp_owner2_first_name'] = None
            df.at[idx, 'temp_owner2_last_name'] = None
        else:
            first_name1, last_name1, first_name2, last_name2, _, _, _ = parse_person_name_simple(name)
            df.at[idx, 'temp_business_name'] = None
            df.at[idx, 'temp_name_type'] = 'person'
            df.at[idx, 'temp_owner1_first_name'] = first_name1
            df.at[idx, 'temp_owner1_last_name'] = last_name1
            df.at[idx, 'temp_owner2_first_name'] = first_name2
            df.at[idx, 'temp_owner2_last_name'] = last_name2
        
        df.at[idx, 'needs_llm'] = False
    
    return df


#Not Currently used - but functionality exists in clean_and_split_owner_names
# def detect_and_correct_name_order(df):
#     """
#     Detects the predominant name order in the DataFrame and corrects any inconsistent entries.

#     Args:
#         df (pd.DataFrame): The DataFrame containing parsed owner names.

#     Returns:
#         pd.DataFrame: The DataFrame with corrected name orders.
#     """
#     # Determine the predominant name order type across the entire DataFrame
#     type_counts = df['name_order_type'].value_counts()
#     if not type_counts.empty:
#         predominant_type = type_counts.idxmax()
#     else:
#         predominant_type = 'first_last'  # Default to 'first_last' if no data
    
#     logger.info(f"Predominant name order detected: {predominant_type}")
    
#     # If the predominant type is 'last_first', swap the first and last names
#     if predominant_type == 'last_first':
#         # Swap owner1 first and last names where necessary
#         swap_condition_owner1 = df['name_order_type'] == 'first_last'
#         df.loc[swap_condition_owner1, ['owner1_first_name', 'owner1_last_name']] = df.loc[swap_condition_owner1, ['owner1_last_name', 'owner1_first_name']].values
        
#         # Swap owner2 first and last names where necessary
#         swap_condition_owner2 = df['name_order_type'] == 'first_last'
#         df.loc[swap_condition_owner2, ['owner2_first_name', 'owner2_last_name']] = df.loc[swap_condition_owner2, ['owner2_last_name', 'owner2_first_name']].values
        
#         # Update the name_order_type to match the predominant_type
#         df['name_order_type'] = predominant_type
    
#     elif predominant_type == 'first_last':
#         # Ensure all entries match 'first_last' if needed
#         # No action needed as 'first_last' is already the desired order
#         pass
#     else:
#         # Handle 'unknown' or other types if necessary
#         logger.warning(f"Unhandled name order type: {predominant_type}")
    
#     return df

# def use_llm_to_parse_names(name):
#     ts = time.time()
#     llm = Ollama(model="llama3")

#     #filepath = 'test_input_expected.csv'
#     #df1=pd.read_csv(filepath)

#     def format_prompt(sample):
#         return f"""
#     {"Ben & Lisa Abernathy"}
#     format:
#     business_name: {None}
#     person_1_first_name: {"Ben"}
#     person_1_last_name: {"Abernathy"}
#     person_2_first_name: {"Lisa"}
#     person_2_last_name: {"Abernathy"}
#     """

#     def query(llm, prompt):
#         response = llm.invoke(prompt) # if max tokens is zero, depends on n_ctx
#         print(response)
#         return response


#     def getnames(res):
#         first_name = []
#         last_name = []
#         # Define regular expressions for first and last names
#         business_name_pattern = re.compile(r'business_name:\.*([^\n]+)')
#         first_name_pattern = re.compile(r'person_\d+_first_name:\.*([^\n]+)')
#         last_name_pattern = re.compile(r'person_\d+_last_name:\.*([^\n]+)')
#         # Extract first names
#         first_name = first_name_pattern.findall(res)
#         # Extract last names
#         last_name = last_name_pattern.findall(res)
#         print(first_name,last_name)
#         # Extract business name
#         business_name_match = business_name_pattern.search(res)
#         business_name = ''
#         if business_name_match:
#             business_name = business_name_match.group(1)
#             o_b_n = business_name
#         else:
#             o_b_n = ""

#         o_1_f_n=""
#         o_2_f_n=""
#         o_1_l_n=""
#         o_2_l_n=""

#         for index in range(len(first_name)):
#             o_1_f_n=""
#             o_2_f_n=""
#             if(index==0):
#                 o_1_f_n=first_name[index]
#             else:
#                 o_2_f_n=first_name[index]

#         for index in range(len(last_name)):
#             o_1_l_n=""
#             o_2_l_n=""
#             if(index==0):
#                 o_1_l_n=last_name[index]
#             else:
#                 o_2_l_n=last_name[index]

#         remove_blanks(o_1_f_n)
#         remove_blanks(o_1_l_n)
#         remove_blanks(o_2_f_n)
#         remove_blanks(o_2_l_n)
#         remove_blanks(o_b_n)
#         names_list=[o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n]
#         ##print("Person {}: First Name: {} Last Name: {}".format(index+1, first_name[index], last_name[index]))
#         return names_list

#     def remove_blanks(name_part):
#         name_part.strip()
#         for i in ['None',' None','nan','(blank)','N/A', 'null']:
#             if name_part == i:
#                 name_part= ""
#     prompt = '''
#     format the data as below examples: 
#     '''
#     prompt = prompt + (format_prompt(name))
#     prompt1 = prompt + f"""

#     {name}
#     """
#     response1 = query(llm, prompt1)
#     response2= getnames(response1)
#     print(response2)
#     return response2

def is_business(row):
    business_names = ['LLC', 'INC', 'FAMILY']
    names = ''
    if 'owner_full_name' in row: 
      names = (str(row['owner_full_name']).upper()).split(' ')
    return True if (np.in1d(business_names, names)).any() else False

def create_derived_columns(df):
    df['num_missing_in_row'] = df.isna().sum(axis=1)
    df['is_name_missing'] = df['full_name'].isna() | (df['full_name'] == '')
    df['is_business'] = df['full_name'].apply(is_business)
    return df

REQUIRED_COLUMNS_GENERIC = [
    "owner_full_name",
    "mail_zip_code",
    "property_zip_code"
]

REQUIRED_COLUMNS_LAND = [
    "lot_area_sqft", "apn"
]

REQUIRED_COLUMNS_SINGLE =[

]

REQUIRED_COLUMNS_COMMERCIAL =[

]

REQUIRED_COLUMNS_MULTI =[

]    

LEAD_SORTING_COLUMNS =['potential_age', 'gender', 'education', 
                       'householdincome' ,'density', 'age_median', 
                       'divorced', 'family_dual_income', 'income_household_median', 
                       'home_value', 'education_some_college', 'race_white', 
                       'num_missing_in_row', 'is_name_missing', 'is_business']
# LEAD_SORTING_COLUMNS =['density', 'age_median', 
#                        'divorced', 'family_dual_income', 'income_household_median', 
#                        'home_value', 'education_some_college', 'race_white', 
#                        'num_missing_in_row', 'is_name_missing', 'is_business']

def apply_pca(df, python_scripts_path, base_file_path, pipeline='train'):
    """
    Apply PCA to the dataframe.
    Saves the PCA model.

    :param df: Input dataframe (preferably standardized).
    :return: PCA-transformed dataframe.
    """
    pca_path = os.path.join(python_scripts_path, base_file_path + "_pca.pkl")
    if pipeline == 'train':
        pca = PCA()
        df_pca = pd.DataFrame(pca.fit_transform(df))
        
        with open(pca_path, "wb") as f:
            pickle.dump(pca, f)
        
        logger.info(f"    * PCA has been applied on train data. PCA pickle file has been saved here -> {pca_path}\n")    
        
    elif pipeline == 'test':
        # Load the PCA model
        with open(pca_path, "rb") as f:
            pca = pickle.load(f) 
            
        # Transform test data using loaded PCA model
        df_pca = pd.DataFrame(pca.transform(df))
        logger.info(f"    * PCA model loaded from - {pca_path}. PCA has been applied on test data.\n")    
        
    pca_columns = ["PCA_" + str(i+1) for i in range(df_pca.shape[1])]
    df_pca.columns = pca_columns
    # Save the PCA model
    return df_pca


# def append_zipcode_data(df, uszips, required_columns):
#     try:
#         zipcolumns = ['density', 'age_median', 'divorced', 'family_dual_income', 'income_household_median', 'home_value', 'education_some_college', 'race_white']
#         # uszips['zip'] = uszips['zip'].astype(str)
#         # print(['zip']+ zipcolumns)
#         # Debugging: Log the shape and columns of df
#         # print("DataFrame shape:", df.shape)
#         # print("DataFrame columns:", df.columns)
#         # print("uszips['zip']:", uszips['zip'])
        
#         logger.info("inside append_zipcode_dataappend_zipcode_data")
#         df = df.merge(uszips[['zip']+ zipcolumns], how='left', left_on=['zipcode'], right_on=['zip'])
#         # logger.info("after", required_columns)
#         del df['zip']
#         required_columns += zipcolumns
#         return df, required_columns
#     except Exception as e:
#         logger.error("Errir in ppend", e)
#         # exc = str(e).replace('"', '').replace("'", '')
#         logger.info("Hit append_zipcode_data: %s", e)
#         raise Exception(e)

def append_zipcode_data(df, uszips):
    try:
        zipcolumns = ['density', 'age_median', 'divorced', 'family_dual_income', 'income_household_median', 'home_value', 'education_some_college', 'race_white']
        logger.info("before append_zipcode_datasss", df)
        df['zipcode'] = df['zipcode'].fillna(0).astype(int)
        df['zipcode'] = df['zipcode'].astype(int)
        logger.info("Inside append_zipcode_data")
        # Verify that both DataFrames have a 'zipcode' column
        if 'zipcode' not in df.columns:
            raise ValueError("DataFrame 'df' does not have a 'zipcode' column.")
        if 'zip' not in uszips.columns:
            raise ValueError("DataFrame 'uszips' does not have a 'zip' column.")

        # Merge the DataFrames
        df = df.merge(uszips[['zip'] + zipcolumns], how='left', left_on=['zipcode'], right_on=['zip'])
        
        # Clean up by removing the 'zip' column from df
        del df['zip']
        
        # Update the list of required columns
        # required_columns += zipcolumns
        
        logger.info("Data merged successfully")
        return df
    except Exception as e:
        logger.error("Error in append_zipcode_data: %s", str(e))
        raise e  # Re-raise the exception for higher-level handling

def handle_zipcode(mail_zip):
  try:
    mail_zip= str(mail_zip).replace('=','').replace('"', '').replace('.0','')
    if not (str(mail_zip)[:5].isdigit()):
      return None
    if(len(str(mail_zip)) ==  5):
      return str(mail_zip)
    elif(len(str(mail_zip)) > 5):
      mail = str(mail_zip)[:5]
      return mail
    elif(len(str(mail_zip)) < 5):
      mail = "0"*(5-len(str(mail_zip))) + str(mail_zip) #add 0 at end of zipcodes which have length less than 5
      return mail    
  except Exception as e:
    logger.error("Error in mail_zip",e)
    return None

def get_first_name(full_name):
    if "," in full_name:
      name_parts = full_name.split(',')
      return name_parts[1].split()[0]
    elif full_name != '':
      return full_name.split()[0]
    else:
      return None

def get_age(row, df):
  # Convert date of birth e.g. 198001 to age
  print("in get_age")
  
  #TODOShyam fix this
  if 'dateofbirth' in df.columns and pd.notnull(row['dateofbirth']):
    
    if len(str(row['dateofbirth'])) >= 4:
        today = datetime.date.today()
        year = today.year
        print(len(str(row['dateofbirth'])))
        year_of_birth = int(str(row['dateofbirth'])[:4])
        print("dob is", row['dateofbirth'])
        age = year - year_of_birth 
        print("age is", age)
        return age
  first_name = None
  try:
    if 'first_name' in df.columns and (pd.notnull(row['first_name'])):
      first_name = row['first_name'].split()[0]
    if first_name == None and ('full_name' in df.columns) and (pd.notnull(row['full_name'])):
      first_name = get_first_name(row['full_name'])
    
    response = requests.get(f"https://api.agify.io?name={first_name}&country_id=US&apikey=01eb88b54c29db19764c2d177d7b25c1", timeout=3)
    try:
      age = response.json()['age']
    except:
      age = 0
    return age
  except Exception as e:
    logger.error('Error in getting age')
    return 0

def remove_duplicate_header_rows(df):
    # Use the `all` method to find rows where all values are equal to the corresponding column names
    mask = (df == df.columns).all(axis=1)
    # Invert the mask using the `~` operator and use it to filter the DataFrame
    df = df[~mask]
    
    return df

def clean_data(df):
    df = df.T.drop_duplicates().T
    df = df.loc[:, df.ne('').any()]
    df.dropna(how='all', inplace=True)
    df = remove_duplicate_header_rows(df)
    # removing column names that are duplicated.
    df = df.loc[:,~df.columns.duplicated()]
    df = df.drop_duplicates()
    return df

def basic_preprocessing(df, type = "train"): 
    logger.info(f"---- Replacing slashes and spaces with an underscore.... {df.shape}")
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
    df.columns = df.columns.str.replace('/', '_')
    logger.info(f"---- Replacing column names with standard column names.... {df.shape}")
    df.rename(columns=COLUMN_REPLACE_DICT, inplace=True)
    
    # if len(feature_columns):
    #     feature_columns = [columns.lower().strip().replace(' ', '_').replace('/', '_') for columns in feature_columns]
    #     feature_columns = [COLUMN_REPLACE_DICT.get(item, item) for item in feature_columns]
    #     logger.info(f"---- Replacing feature_columns with standard column names.... {feature_columns}")
    # if type == "train":
    #     df = df.T.drop_duplicates().T
    #     logger.info(f"---- Removing duplicate columns and rows.... {df.shape} \n")
    # TODOO: confirm with Madhur what below code is doing

    # Use the `all` method to find rows where all values are equal to the corresponding column names
    mask = (df == df.columns).all(axis=1)
    
    # removing column names that are duplicated.
    df = df.loc[:,~df.columns.duplicated()]
    
    # Invert the mask using the `~` operator and use it to filter the DataFrame
    df = df[~mask]
    df = df.drop_duplicates()
    return df

def strip_non_numeric_and_convert_to_float(df, column_name):
    df[column_name] = df[column_name].fillna(0)
    df[column_name] = df[column_name].apply(lambda x: re.sub(r'[^0-9.]', '', str(x))).astype(float)
    return df

def abbrevate_state_names(state):
  try: 
    state = str(state)
    if(len(state) == 2):
        return state
    else:
      if state.title() in us_state_to_abbrev:
        return us_state_to_abbrev[state] #get the abbrevation and assign it to the state names
      else:
        return None
  except Exception as e:
    logger.error("State not found ", e)
    return None
def extract_zip_code(address):
    if (pd.isna(address)):
        return None
    address_parts = address.split()
    zip_code = re.search(r'\b\d{5}(?:[-\s]\d{4})?\b', address_parts[-1])
    if zip_code:
        return zip_code.group(0)
    else:
        return None

def extract_mail_city_state(data):
    mail_city = None
    mail_state = None
    if (pd.isna(data)):
        return None, None
    mail_city_state_parts = data.split()
    if (" ".join(mail_city_state_parts[:-1])):
        mail_city = " ".join(mail_city_state_parts[:-1])
    if (mail_city_state_parts[-1]):
        mail_state = mail_city_state_parts[-1]
    return pd.Series([mail_city, mail_state])

def find_zip_code_from_county(dataframe, row):
    try:
        county_name = row["property_county"].split(", ")[0].title()
        state_name = row["property_state_name_short_code"].upper()
        # returning zip code from first row of filtered data frame by keywords

        return dataframe.loc[
            dataframe['COUNTYNAME'].str.contains(county_name) &
            dataframe['STATE'].str.contains(state_name)
        ].iloc[0]["ZIP"].item()
    except:
        return None
def is_full_address(address):
    if (pd.isna(address)):
        return None
    # Define regular expressions to match a city, state, and zip code
    #Commenting this out because for some reason doesn't catch city in "711 CEDAR SPRINGS DR, JACKSONVILLE, AL 36265-6031", possibly due to the ,
    #city_regex = r'\b[A-Z][a-z]+\b'
    state_regex = r'\b[A-Z]{2}\b'
    zip_regex = r'\b\d{5}(?:-\d{4})?\b'
    
    # Use regular expressions to check if the address contains a city, state, and zip code
    contains_state = bool(re.search(state_regex, address))
    contains_zip = bool(re.search(zip_regex, address))
    # If all three are present, it's a full address; otherwise, it's just a street address
    if contains_state and contains_zip:
        return True
    else:
        return False

def create_columns_if_doesnt_exist(df):
    logger.info(f"   6.1 Creating full_name columns... {df.shape}")
    # Check if 'full_name' is in the dataframe columns
    if 'full_name' not in df.columns:
        # If 'full_name' is not present, check if 'first_name' and 'last_name' are both present
        if 'first_name' in df.columns:
            df['full_name'] = df['first_name']
            if 'last_name' in df.columns:
                # If 'first_name' and 'last_name' are present, combine them to create 'full_name'
                df['full_name'] += ' ' + df['last_name']
        elif 'last_name' in df.columns:
            df['full_name'] = df['last_name']
        # else:
        #     raise Exception("The file doesn't have 'full_name' or both 'first_name' and 'last_name'.")
    
    logger.info(f"   6.2 Creating full_address columns... {df.shape} \n")
    
    # Check if 'full_address' is in the dataframe columns
    if 'full_address' not in df.columns:
        if 'street_address' in df.columns:
            df['full_address'] = df['street_address']
            
            # Check if either 'state' or 'state_(shipping)' column is present
            if 'state' in df.columns:
                df['full_address'] += ' ' + df['state']
            # elif 'state_(shipping)' in df.columns:
            #     df['full_address'] += ' ' + df['state_(shipping)']
            
            # Check if 'zipcode' column is present
            if 'zipcode' in df.columns:
                df['full_address'] += ' ' + df['zipcode'].astype(str)
    logger.info(f"   6.3 Creating owner_full_name columns... {df.shape} \n")
    if not 'owner_full_name' in df.columns:
        if (('owner_first_name') in df.columns) and (('owner_last_name') in df.columns):
            df['owner_full_name'] = df['owner_first_name'] + ' ' + df['owner_last_name'] 
        elif ('owner_mailing_name' in df.columns):
            df['owner_full_name'] = df['owner_mailing_name']
        elif ('owner_first_name' in df.columns):
            df['owner_full_name'] = df['owner_first_name']     
    logger.info(f"   6.4 Creating lot_area_sqft columns... {df.shape} \n")
    if (not 'lot_area_sqft' in df.columns) and ('lot_acreage' in df.columns):
        df = strip_non_numeric_and_convert_to_float(df, 'lot_acreage')
        df['lot_area_sqft'] = df['lot_acreage'].apply(pd.to_numeric) * 43560
    if (not 'lot_area_sqft' in df.columns) and (not 'lot_acreage' in df.columns) and ('lot_area' in df.columns):
        df = strip_non_numeric_and_convert_to_float(df, 'lot_area')
        if df['lot_area'].mean() < 100:
            df['lot_area_sqft'] = df['lot_area'].apply(pd.to_numeric) * 43560
        else:
            df['lot_area_sqft'] = df['lot_area']
    logger.info(f"   6.5 Creating property_state_name_short_code columns... {df.shape} \n")
    if 'property_state_name_short_code' in df.columns:
      df['property_state_name_short_code'] =  df.apply(lambda row: abbrevate_state_names(row['property_state_name_short_code']), axis=1)
    if 'mail_state_name_short_code' in df.columns:
        df['mail_state_name_short_code'] = df.apply(lambda row: abbrevate_state_names(row['mail_state_name_short_code']), axis=1)
    logger.info(f"   6.6 Creating property_zip_code columns... {df.shape} \n")
    if (not 'property_zip_code' in df.columns):
        if ('property_address_full' in df.columns):
            df['property_zip_code'] = df.apply(lambda row: extract_zip_code(row['property_address_full']), axis=1)
        elif ('property_county' in df.columns) and ('property_state_name_short_code' in df.columns):
            zip_county = pd.read_csv(os.path.join(pathlib.Path(__file__).parent.resolve(), 'ZIP-COUNTY-FIPS_2017-06.csv'), low_memory = False)
            # iterate through each row of dataframe and populate the newly added column with zip codes after lookup
            df["property_zip_code"] = df.apply(lambda row: find_zip_code_from_county(zip_county, row), axis = 1) 
    logger.info(f"   6.7 Creating mail_full_address columns... {df.shape} \n")
    if ('mail_full_address') in df.columns:
        df['is_full_address'] = df.apply(lambda row: is_full_address(row['mail_full_address']), axis=1)
        try:
            if ((df['is_full_address']==False).sum()/df.shape[0] > 0.5):
                df.rename(columns={'mail_full_address': 'mail_street_address'}, inplace=True)
        except:
            pass
    logger.info(f"   6.8 Creating mail_zip_code columns... {df.shape} \n")
    if (not 'mail_zip_code' in df.columns):
        if ('mail_full_address' in df.columns):
            df['mail_zip_code'] = df.apply(lambda row: extract_zip_code(row['mail_full_address']), axis=1)
        if ('owner_city_state_zip' in df.columns):
            df['mail_zip_code'] = df.apply(lambda row: extract_zip_code(row['owner_city_state_zip']), axis=1)
    logger.info(f"   6.9 Creating mail_city_state columns... {df.shape} \n")
    if ('mail_city_state' in df.columns and 'mail_state_name_short_code' not in df.columns):
        df[['mail_cty', 'mail_state_name_short_code' ]] = df.apply(lambda row: extract_mail_city_state(row['mail_city_state']), axis=1)
    return df

def preprocess_data(df, feature_columns):
   
    # Check and remove rows that are column names
    for col in df.columns:
        df = df[df[col] != col]
        df[col] = pd.to_numeric(df[col], errors='ignore')
    
    """
    # TODO: Why do we drop stuff?
    # Remove rows if all data is null or empty or rows where all elements are same
    df = df.dropna(how='all')  # This removes rows where all elements are NaN
    df = df[~(df.applymap(lambda x: x == '' if isinstance(x, str) else False).all(axis=1))]
    df = df[df.apply(lambda row: len(row.unique()) > 2, axis=1)]
    
    # TODO: Why do we need this? We should retain columns
    # Keep only those columns that are present in feature_columns
    columns_to_keep = [col for col in df.columns if col in feature_columns]
    df = df[columns_to_keep]
    """

    # Use logger to indicate feature_columns not present in the dataframe
    missing_features = [col for col in feature_columns if col not in df.columns]
    if missing_features:
        logger.info(f"---- The following feature_columns are not present in the dataframe: {', '.join(missing_features)}")
    
    return df

def read_file(filename):
    df = None
    # Check if the filename ends with '.xlsx' or '.csv'
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        # Use pandas read_excel function to read the file
        df = pd.read_excel(filename)
    elif filename.endswith('.csv'):
        encodings = ["utf-8", "utf-8-sig", "latin1", "iso-8859-1", "cp1252"]
        # Use pandas read_csv function to read the file
        for encoding in encodings:
            try:
                cols = pd.read_csv(filename, nrows=1).columns
                df = pd.read_csv(filename, encoding=encoding,  usecols=cols)
                break
            except Exception as e:
                pass
    else:
        # If the file is not .xlsx or .csv, print a message and return None
        raise Exception("---- The file must be a .xlsx or .xls or .csv file...\n")
    # Return the dataframe
    return df

# def check_columns(df):
#     logger.info(f"---- Checking if the required columns are present.... {df.shape}")
#     # Check if the required columns are present
#     missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]

#     # If no columns are missing, log a success message
#     if not missing_columns:
#         logger.info(f'---- Processing success: All required columns are present.... {df.shape}')
#         df.reset_index(inplace=True,drop=True)
#         logger.info(f"---- Checking relevant columns. Creating them if it's not present.... {df.shape} \n")
#         for col in RELEVANT_COLUMNS:
#             if col not in df.columns:
#                 df[col] = None
#         # df = df[RELEVANT_COLUMNS]
#     # If there are missing columns, raise an exception and log an error
#     else:
#         for col in missing_columns:
#             logger.error(f'---- Missing column: {col}...')
#         raise ValueError(f'---- Missing columns: {missing_columns}')

#     return df

def required_columns_exist(df, industry_profile, feature_columns=[], module_type = "v1.1"):
    industry_type = "real_estate_investors"
    missing_columns = []

    if "industry_type" in industry_profile:
        industry_type = industry_profile["industry_type"]
    if industry_type == "real_estate_investors" and module_type == "v1" :
        # true_required_columns = REQUIRED_COLUMNS_GENERIC
        true_required_columns = REQUIRED_COLUMNS
    elif module_type == "v1.1": 
        true_required_columns = REQUIRED_COLUMNS_V_11_2
    else:
        # true_required_columns = REQUIRED_COLUMNS_GENERIC
        true_required_columns = REQUIRED_COLUMNS
    
    #TODOO: if v1 then existing code elif v1.1 or v2 then check if feature columns present or not
    # and if full_name and full_address be present in df.columns
    print("true_required_columns", true_required_columns)
    logger.info(f"---- Checking relevant columns111.")
    if module_type == "v1":
        if industry_profile and 'land' in industry_profile['property_type']:
            true_required_columns = true_required_columns + REQUIRED_COLUMNS_LAND
        if industry_profile and 'single' in industry_profile['property_type']:
            true_required_columns = true_required_columns + REQUIRED_COLUMNS_SINGLE
        if industry_profile and 'commercial' in industry_profile['property_type']:
            true_required_columns = true_required_columns + REQUIRED_COLUMNS_COMMERCIAL 
        if industry_profile and 'multi' in industry_profile['property_type']:
            true_required_columns = true_required_columns + REQUIRED_COLUMNS_MULTI    
    else:
        true_required_columns = true_required_columns + feature_columns

    print("true_required_columns", true_required_columns)
    logger.info(f"---- Checking relevant columns1111212.")
    for column_name in true_required_columns:
        if column_name not in df.columns:
            missing_columns.append(column_name)
    source = ""
    print("missing_columnsmissing_columns", missing_columns)
    if missing_columns:
        error_message = "column_mapping.py -- Required columns don't exist for " + source + " file, columns missing are: " + ', '.join(missing_columns)
        raise Exception(error_message)
    # else:
    #     df.reset_index(inplace=True,drop=True)
    #     logger.info(f"---- Checking relevant columns. Creating them if it's not present.... {df.shape} \n")
    #     for col in RELEVANT_COLUMNS:
    #         if col not in df.columns:
    #             df[col] = None
    return df
    
def clean_zipcode(df, training=False):
    try:
        # Convert 'zipcode' column to string
        df['zipcode'] = df['zipcode'].astype(str)
        # Extract the first sequence of 5 digits from each zipcode
        df['zipcode'] = df['zipcode'].str.extract(r'(\b\d{5}(?:[-\s]\d{4})?\b)')
        df['zipcode'] = df['zipcode'].astype(str)
        df['zipcode'] = df['zipcode'].str.strip()
        df.loc[df['zipcode'].isna(), 'zipcode'] = df.loc[df['zipcode'].isna(), 'full_address'].str.extract(r'(\b\d{5}(?:[-\s]\d{4})?\b)')
        if training:
            df['zipcode'].dropna(inplace=True)
            df = df[df['zipcode'] != ' ']
            # df = df[(df['zipcode'] != '')]
            # df = df[['zipcode'] != ' ']
            df = df[~df['zipcode'].isna()]
            df = df[df['zipcode'] != 'nan']
        return df
    except Exception as e:
            logger.error("Error in cleaning zipcode function clean_zipcode:", exc_info=True)
            raise(e)
    
    

def clean_name(df):
    # Regex pattern for non-alphanumeric characters excluding '.' and '_'
    pattern = r"[^a-zA-Z.,' -]"
    # Replace non-alphanumeric characters (excluding '.' and '_') in 'name' column with empty string
    df['full_name'] = df['full_name'].str.replace('(hidden)', '', regex=False)
    df['full_name'] = df['full_name'].str.replace(pattern, '', regex=True)
    df['full_name'] = df['full_name'].str.replace('\s+', ' ', regex=True)  
    return df

def clean_email(df):
    # Regex pattern for email
    pattern = r'([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-\.]+\.[a-zA-Z0-9-]+)'

    # Assuming 'text_column' is the column where you want to extract email from
    df['email_address'] = df['email_address'].astype(str)
    df['email_address'] = df['email_address'].str.extract(pattern)
    df['email_address'] = df['email_address'].str.strip()
    return df

def select_data(df, max_rows=None):
    # Calculate weights for each zipcode
    weights = df['zipcode'].value_counts(normalize=True)

    # Map weights to the dataframe
    df['weights'] = df['zipcode'].map(weights)
    
    # if not max_rows:
        # max_rows=df.shape[0] - df['weights'].isnull().sum()
    if not max_rows:
        max_rows = df.shape[0] - df['weights'].isnull().sum()
    else:
        max_rows = min(max_rows, df.shape[0] - df['weights'].isnull().sum())  # Ensure max_rows doesn't exceed the number of records

    # Sample 1000 rows representing the 'zipcode' column
    df = df.sample(n=max_rows, weights='weights', replace=False, random_state=42)
    
    # You can drop the 'weights' column afterwards if you don't need it
    df = df.drop(columns=['weights'])
    return df


def processAddress(fullAddress):
    if pd.isna(fullAddress) or not fullAddress:
        return None, None, None, None
    parsed = usaddress.parse(fullAddress)
    # print('parsedparsedparsedparsed', parsed)
    line1 = ""
    zipCode = None
    state = None
    city = None
    possibleMetaTagsForLine1 = ['AddressNumber', 'StreetNamePreDirectional', 'StreetName', 'StreetNamePostType', 'USPSBoxID', 'USPSBoxType', 'USPSBoxType', 'StreetNamePreModifier', 'StreetNamePreType', 'OccupancyIdentifier', 'OccupancyIdentifier']
    
    # Additional variable to concatenate PlaceName values
    placeName = ""
    
    for i in parsed:
        if i[1] in possibleMetaTagsForLine1:
            line1 += " " + i[0]
        if i[1] == 'ZipCode':
            zipCode = i[0]
            if '-' in zipCode:
                zipCode = zipCode.split('-')[0]  # Extract main ZIP code without the additional characters
        if i[1] == 'StateName':
            state = i[0]
        if i[1] == 'PlaceName':
            # Concatenate PlaceName values
            if city:
                city += " " + i[0]
            else:
                city = i[0]
    
    if line1:
        line1 = line1.lstrip()
    if city:
        city = city.replace(',', '')
    
    return line1, zipCode, city, state


"""Function that calls Melissa API and gets person data"""
def get_melissa_person_data(row):
   
    try:
        full_name = row.get('full_name', None)
        if 'full_address' in row:
            street_address, zip_code, city, state = processAddress(row['full_address'])
    except:
        return row

    if any(pd.isnull(value) for value in [ street_address, zip_code]):
        return row

    street_address_cleaned = street_address.replace("#", "")

    full_name_encoded = None
    if full_name is not None:
        full_name_cleaned = full_name.replace("#", "").replace("C/O", "&") 
        full_name_encoded = full_name_cleaned.replace(" ", "%20") 


    street_address_encoded = street_address_cleaned.replace(" ", "%20")
    city_encoded = city.replace(" ", "%20") if city is not None else None

    response = None
    if demographicDataCollection.count_documents({"original_full_name": full_name, "title_case_street_address": street_address.title()}) > 0:
        response = demographicDataCollection.find_one({"original_full_name": full_name, "title_case_street_address": street_address.title()})
        response = response
    else:
        try:
            melissa_key = 'cKEf_4HZGh_L5EV98iXjXC**'
            melissa_url = f"https://personator.melissadata.net/v3/WEB/ContactVerify/doContactVerify?t=&id={melissa_key}"
            cols = "AddressLine1,AddressLine2,AddressStreetName,CountryCode,DateOfBirth,Gender,DateOfDeath,Education,HouseholdIncome"
            melissa_url += f"&cols={cols}"

            # Dynamically generate query parameters
            params = {
                'full': full_name_encoded,
                'comp': '',
                'a1': street_address_encoded,
                'a2': '',
                'city': city_encoded,
                'state': state,
                'postal': zip_code,
                'email': '',
                'phone': '',
                'ctry': '',
                'ss': '',
                'mak': '',
                'ip': '',
                'bday': '',
                'bmo': '',
                'byr': '',
                'format': 'json'
            }

            complete_url = melissa_url + '&' + '&'.join(f"{k}={v}" for k, v in params.items() if v is not None)

            response = requests.get(complete_url, timeout=5)
            if response.status_code == 200 and response.headers["content-type"].strip().startswith("application/json"):
                response = response.json()['Records'][0]
                response['original_full_name'] = full_name
                response['title_case_street_address'] = street_address.title()
                response['uploaded_at'] = datetime.now(pytz.utc)

                x = demographicDataCollection.insert_one(response)
            else:
                return row
        except Exception as e:
            logger.error("hit error fetching data from Melissa", e)
            return row

    #res = response['Results']
    row["gender"] = response["Gender"]
    row["education"] = response["Education"]
    row["householdincome"] = response["HouseholdIncome"]
    row["dateofbirth"] = response["DateOfBirth"]

    return row

#TODO @Harshit, a function similar to this is used in v1, pls refer v1's function and see if we can bring that here.
# that one is more tested. But we need to ensure that if Melissa fails we don't drop the entire row 
def append_data(df):
    
    df = df.merge(df.apply(lambda row: pd.Series(get_melissa_person_data(row)), axis = 1))
    
    # Apply the function to the column and create a new column
    df['potential_age'] = df.apply(lambda row: get_age(row, df), axis = 1)
    logger.info("---- Created new dataframe fetched data.... \n")
    required_columns = ["potential_age", "education", "gender", "householdincome"]
    return df, required_columns

def get_person_details_from_email(email):
    try:
        # Define Melissa Data API key
        melissa_key = 'cKEf_4HZGh_L5EV98iXjXC**'
        melissa_url = f"https://personator.melissadata.net/v3/WEB/ContactVerify/doContactVerify?t=&id={melissa_key}"
        cols = "AddressLine1,AddressLine2,AddressStreetName,CountryCode,DateOfBirth,Gender,DateOfDeath,Education,HouseholdIncome,PhoneNumber"
        melissa_url += f"&cols={cols}"
        
        # Construct query parameters as a dictionary
        params = {
            'email': email,
            'format': 'json',
            'cols': cols,
            'act': "Append"
            # Add other parameters as needed
        }
        complete_url = melissa_url + '&' + '&'.join(f"{k}={v}" for k, v in params.items() if v is not None)
        # print(complete_url)
        
        # Make the API request
        response = requests.get(complete_url, timeout=20)
        
        if response.status_code == 200 and response.headers["content-type"].strip().startswith("application/json"):
            data = response.json()['Records'][0]
            
            # You can directly return the entire data dictionary
            return data
        else:
            # If the request fails, return None or raise an exception as needed
            return None
    except Exception as e:
        # Handle exceptions here if needed
        print(f"Error: {e}")
        return None

# Add after the existing strip_name function
def extract_name_suffix(name):
    """
    Extracts and removes suffixes from a name, returning the cleaned name and any found suffix.
    
    Args:
        name (str): The input name to process
        
    Returns:
        tuple: (cleaned_name, suffix)
    """
    # Common name suffixes (case insensitive)
    GENERATIONAL_SUFFIXES = [
        r'\bJR\.?\b', r'\bSR\.?\b', r'\bI{2,3}\b',  # Jr, Sr, II, III
        r'\bIV\b', r'\bV\b', r'\bVI\b',  # IV, V, VI
        r'\bJUNIOR\b', r'\bSENIOR\b'  # Junior, Senior
    ]
    
    if pd.isna(name) or not isinstance(name, str):
        return name, None
        
    name = name.strip()
    suffix = None
    
    # Try to find and extract any suffix
    for pattern in GENERATIONAL_SUFFIXES:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            suffix = match.group()
            # Remove the suffix and any extra spaces/commas
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
            name = re.sub(r'\s*,\s*$', '', name)
            name = re.sub(r'\s+', ' ', name).strip()
            break
            
    return name, suffix

def setup_llm():
    """Initialize the Llama model and tokenizer from local path."""
    try:
        model_path = os.getenv('LLM_MODEL_PATH', '/usr/src/app/.sandbox/Llama-3.2-1B-Instruct')
        offload_path = os.getenv('LLM_OFFLOAD_PATH', os.path.join(os.path.dirname(model_path), 'llm_offload_folder'))
        logger.info(f"Loading model from path: {model_path}")
        logger.info(f"Using offload folder: {offload_path}")
        
        # Create offload directory if it doesn't exist
        os.makedirs(offload_path, exist_ok=True)
        
        # Verify model path exists and contains required files
        if not os.path.exists(model_path):
            raise ValueError(f"Model path does not exist: {model_path}")
        for required_file in ['config.json', 'tokenizer.json', 'model.safetensors']:
            if not os.path.exists(os.path.join(model_path, required_file)):
                raise ValueError(f"Required file {required_file} not found in {model_path}")

        # Load tokenizer with explicit offline mode
        tokenizer = AutoTokenizer.from_pretrained(
            model_path,
            local_files_only=True,
            trust_remote_code=True
        )

        # Load model with explicit offline mode and disk offloading
        model = AutoModelForCausalLM.from_pretrained(
            model_path,
            local_files_only=True,
            trust_remote_code=True,
            torch_dtype=torch.float32,
            device_map="auto",
            offload_folder=offload_path
        )
        
        tokenizer.pad_token_id = tokenizer.eos_token_id
        
        pipe = pipeline(
            "text-generation",
            model=model,
            tokenizer=tokenizer,
            max_new_tokens=32,
            temperature=0.1, 
            do_sample=False
        )
        
        logger.info("Model and tokenizer loaded successfully")
        return pipe, tokenizer
        
    except Exception as e:
        logger.error(f"Error setting up LLM: {e}")
        logger.error(f"Current working directory: {os.getcwd()}")
        logger.error(f"Model path contents: {os.listdir(model_path) if os.path.exists(model_path) else 'Directory not found'}")
        raise


def verify_business_name(pipe, tokenizer, name):
    """Verify if a name is actually a business name."""
    messages = [
        {
            "role": "system",
            "content": """You are a precise name classifier that determines if a name represents a business/organization or a person.

Rules for classification:
- Business names include:
  * Government entities (United States Of America, County/City names)
  * Financial institutions (Bank names with NA, Federal/National associations)
  * Educational institutions (Schools, Universities)
  * Organizations with numbers (Lodge 803, Chapter 123)
  * Business suffixes: LLC, Inc, Corp, LP, Co, NA, Ltd
  * Multiple people joined by: Brothers, & Son, Family
  * Location-based names: Summit County Land, Embassy Park
  * Abbreviated terms: Natl, Mtg, Assn, Cnty
  
- Person names include:
  * Single letters or initials
  * Names in format: Last First Middle
  * Names with suffixes: Jr, Sr, III
  * Names with middle initials
  * Professional titles: Dr, Mr, Mrs, Esq

Respond ONLY with one of these exact words:
BUSINESS = For companies, organizations, government entities, institutions
PERSON = For individual names or single initials"""
        },
        {
            "role": "user",
            "content": "Classify this name: Federal Natl Mtg Assn"
        },
        {
            "role": "assistant",
            "content": "BUSINESS"
        },
        {
            "role": "user",
            "content": "Classify this name: Summit Cnty Land Reutilization Corp"
        },
        {
            "role": "assistant",
            "content": "BUSINESS"
        },
        {
            "role": "user",
            "content": "Classify this name: A"
        },
        {
            "role": "assistant",
            "content": "PERSON"
        },
        {
            "role": "user",
            "content": "Classify this name: Smith Robert J Jr"
        },
        {
            "role": "assistant",
            "content": "PERSON"
        },
        {
            "role": "user",
            "content": "Classify this name: Bippert Brothers"
        },
        {
            "role": "assistant",
            "content": "BUSINESS"
        },
        {
            "role": "user",
            "content": f"Classify this name: {name}"
        }
    ]
    prompt = tokenizer.apply_chat_template(messages, tokenize=False)
    response = pipe(prompt)[0]['generated_text']
    response = response.split('assistant\n\n')[-1].strip().upper()
    response = response.replace("'", "").replace('"', '')
    
    logger.info(f"LLM Input: '{name}' -> Classification: '{response}'")
    
    if 'PERSON' in response:
        return False
    elif 'BUSINESS' in response:
        return True
    else:
        logger.warning(f"Unclear LLM response for '{name}': {response}")
        return False


# Use AI to parse a name into first and last
def parse_person_name_simple(name):
    """
    Simplified version of name parsing that focuses only on splitting individual names.
    Does not handle business/trust detection or provided first/last name cases.
    
    Args:
        full_name (str): The full name to parse
    
    Returns:
        tuple: (o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, type, needs_llm)
    """
    # Initialize return values
    o_1_f_n = None 
    o_2_f_n = None
    o_1_l_n = None
    o_2_l_n = None
    o_b_n = None
    name_type = 'unknown'
    needs_llm = False
    
    if pd.isna(name) or name == '':
        logger.debug("Empty or null name provided")
        return o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, name_type, needs_llm
    
    # Handle repeated '&' symbols and clean the name
    name = re.sub(r'&\s*&', '&', name.strip())
    name, suffix = extract_name_suffix(name)

    # Clean up name, strip anything but letters, &, - and ,
    to_keep = set('abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ&-,')
    name = ''.join(filter(to_keep.__contains__, name))
    name_1 = None
    name_2 = None
    
    # Determine if there are multiple owner names
    if '&' in name:
        name_1, name_2 = name.split('&', 2)[:2]
        logger.debug(f"Split on '&': '{name_1}' and '{name_2}'")
    elif ' AND ' in name:
        name_1, name_2 = name.split(' AND ', 2)[:2]
        logger.debug(f"Split on ' AND ': '{name_1}' and '{name_2}'")
    elif ' and ' in name:
        name_1, name_2 = name.split(' and ', 2)[:2]
        logger.debug(f"Split on ' and ': '{name_1}' and '{name_2}'")
    
    # Parse individual names
    if name_1:
        name_1 = name_1.strip()
        o_1_f_n, o_1_l_n, type1 = parse_name(name_1) if name_1 else (None, None, 'unknown')
        name_type = type1  # Set name type from first name parsing
        
        if name_2:
            name_2 = name_2.strip()
            o_2_f_n, o_2_l_n, type2 = parse_name(name_2) if name_2 else (None, None, 'unknown')
            # If both names are parsed successfully, use type2 only if type1 is unknown
            if type2 != 'unknown' and type1 == 'unknown':
                name_type = type2
            
            # Improved shared last name logic
            if ('&' in name or ' and ' in name.lower()):
                # Only apply shared last name logic if:
                # 1. Both names are parsed successfully
                # 2. The last names are the same or one is missing
                if o_1_f_n and o_2_f_n:
                    if o_1_l_n and o_2_l_n and o_1_l_n == o_2_l_n:
                        # Names already share last name, no action needed
                        pass
                    elif o_1_l_n and not o_2_l_n:
                        # Second person missing last name, use first person's
                        o_2_l_n = o_1_l_n
                    elif o_2_l_n and not o_1_l_n:
                        # First person missing last name, use second person's
                        o_1_l_n = o_2_l_n
                    elif not o_1_l_n and not o_2_l_n:
                        # Both missing last name, try to extract from full name
                        words = name.split()
                        potential_last_name = words[-1].strip()
                        # Only use if it's not already used as a first name
                        if potential_last_name not in [o_1_f_n, o_2_f_n]:
                            o_1_l_n = o_2_l_n = potential_last_name
    else:
        o_1_f_n, o_1_l_n, type1 = parse_name(name)
        name_type = type1  # Set name type from single name parsing

    # If both owner names are the same, drop owner 2 name 
    if o_1_f_n == o_2_f_n and o_1_l_n == o_2_l_n:
        o_2_f_n = None
        o_2_l_n = None
    
    # Add suffix if present
    if suffix:
        logger.debug(f"Adding suffix '{suffix}' to appropriate name")
        if o_1_l_n and not o_2_l_n:  # Single owner
            o_1_l_n = f"{o_1_l_n} {suffix}"
        elif o_2_l_n:  # Two owners, suffix goes with last person
            o_2_l_n = f"{o_2_l_n} {suffix}"
        elif o_b_n:  # Business name
            o_b_n = f"{o_b_n} {suffix}"

    logger.debug(f"Final result: {(o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n)}")
    return o_1_f_n, o_1_l_n, o_2_f_n, o_2_l_n, o_b_n, name_type, needs_llm

def get_llm():
    """Lazy loading of LLM model."""
    global _llm_pipe, _llm_tokenizer
    if _llm_pipe is None or _llm_tokenizer is None:
        _llm_pipe, _llm_tokenizer = setup_llm()
    return _llm_pipe, _llm_tokenizer

# def standardize_name_order(df):
#     """
#     Detects the predominant name order pattern in the dataset and standardizes all names to that format.
    
#     Args:
#         df (pd.DataFrame): DataFrame containing parsed names with their order types
        
#     Returns:
#         pd.DataFrame: DataFrame with standardized name order
#     """
#     logger.info("Standardizing name order across dataset...")
    
#     # Get the predominant type (excluding 'unknown', 'business', and empty values)
#     valid_types = df[df['temp_name_type'].isin(['first_last', 'last_first'])]
#     if valid_types.empty:
#         logger.info("No valid name types found for standardization")
#         return df
        
#     type_counts = valid_types['temp_name_type'].value_counts()
#     if type_counts.empty:
#         logger.info("No name patterns to standardize")
#         return df
        
#     predominant_type = type_counts.index[0]
#     logger.info(f"Predominant name type detected: {predominant_type}")
    
#     # Only process rows that have a different valid type
#     mask = (df['temp_name_type'] != predominant_type) & (df['temp_name_type'].isin(['first_last', 'last_first']))
    
#     # Swap names for rows that need to be changed
#     for idx in df[mask].index:
#         # Swap owner 1 names
#         df.loc[idx, ['temp_owner1_first_name', 'temp_owner1_last_name']] = \
#             df.loc[idx, ['temp_owner1_last_name', 'temp_owner1_first_name']].values
            
#         # Swap owner 2 names if they exist
#         swap_condition_owner2 = df['temp_name_type'] == 'first_last'
#         df.loc[swap_condition_owner2, ['temp_owner2_first_name', 'temp_owner2_last_name']] = df.loc[swap_condition_owner2, ['temp_owner2_last_name', 'temp_owner2_first_name']].values
        
#         # Update the type to predominant type after standardization
#         df.loc[idx, 'temp_name_type'] = predominant_type
    
#     logger.info(f"Standardized {mask.sum()} names to {predominant_type} format")
#     return df

def standardize_name_order_LLM(df):
    """
    Uses LLM to detect and standardize name orders, with special attention to international names.
    Uses confidence scores to decide when to override parse_name decisions.
    """
    logger.info("Standardizing name order across dataset using LLM...")
    
    # Get the predominant type (excluding 'unknown', 'business', and empty values)
    valid_types = df[df['temp_name_type'].isin(['first_last', 'last_first'])]
    if valid_types.empty:
        logger.info("No valid name types found for standardization")
        return df
        
    type_counts = valid_types['temp_name_type'].value_counts()
    if type_counts.empty:
        logger.info("No name patterns to standardize")
        return df
        
    predominant_type = type_counts.index[0]
    logger.info(f"Predominant name type detected: {predominant_type}")
    
    # Process both unknown and determined types, but with different confidence thresholds
    mask = ((df['temp_name_type'].isin(['first_last', 'last_first', 'unknown'])))
    
    def get_llm_name_order(first, last, current_type):
        """
        Helper function to query LLM for name order prediction with cultural awareness
        Returns predicted order and confidence level
        """
        if pd.isna(first) or pd.isna(last):
            return None, 0
            
        messages = [
            {
                "role": "system",
                "content": """You are a name order analyzer that determines if a name represents a first_last or last_first order.
Consider common naming patterns and cultural variations. Analyze the name parts carefully.
Only respond in this exact format (no other text):
order: first_last
confidence: high

OR

order: last_first 
confidence: high"""
            },
            {
                "role": "user",
                "content": f"Name parts: first='{first}', last='{last}', type='{current_type}'"
            }
        ]
        
        try:
            pipe, tokenizer = get_llm()
            prompt = tokenizer.apply_chat_template(messages, tokenize=False)
            logger.info(f"LLM Input - Name parts: first='{first}', last='{last}', current_type='{current_type}'")
            
            response = pipe(prompt)[0]['generated_text']
            logger.debug(f"LLM Response: {response}")
            
            # Parse LLM response
            order = None
            confidence = 'low'
            
            if 'order: ' in response:
                order = response.split('order: ')[1].split('\n')[0].strip()
            if 'confidence: ' in response:
                confidence = response.split('confidence: ')[1].split('\n')[0].strip()
                
            # Convert confidence to numeric score
            confidence_scores = {'high': 0.9, 'medium': 0.6, 'low': 0.3}
            confidence_score = confidence_scores.get(confidence, 0.3)
            
            logger.info(f"Name Order Analysis - Order: {order}, Confidence: {confidence} ({confidence_score})")
            return order, confidence_score
            
        except Exception as e:
            logger.error(f"Error querying LLM for name order: {e}")
            return None, 0
    
    # Process each row that needs standardization
    standardized_count = 0
    for idx in df[mask].index:
        current_type = df.loc[idx, 'temp_name_type']
        
        # Set confidence threshold based on current type
        confidence_threshold = 0.9 if current_type in ['first_last', 'last_first'] else 0.6
        
        # For owner 1
        if pd.notna(df.loc[idx, 'temp_owner1_first_name']) and pd.notna(df.loc[idx, 'temp_owner1_last_name']):
            current_first = df.loc[idx, 'temp_owner1_first_name']
            current_last = df.loc[idx, 'temp_owner1_last_name']
            
            predicted_order, confidence = get_llm_name_order(current_first, current_last, current_type)
            logger.info(f"Owner 1 Analysis - Name: {current_first} {current_last}, Predicted: {predicted_order}, Confidence: {confidence}")
            
            # Only override if:
            # 1. We have sufficient confidence
            # 2. Current type doesn't match predominant type
            # 3. Predicted order matches our predominant type
            if (predicted_order and 
                confidence >= confidence_threshold and 
                current_type != predominant_type and 
                predicted_order == predominant_type):
                
                standardized_count += 1
                logger.info(f"Standardizing Owner 1 - Original: {current_first} {current_last} -> Swapping to match {predominant_type}")
                df.loc[idx, ['temp_owner1_first_name', 'temp_owner1_last_name']] = \
                    df.loc[idx, ['temp_owner1_last_name', 'temp_owner1_first_name']].values
        
        # For owner 2
        if pd.notna(df.loc[idx, 'temp_owner2_first_name']) and pd.notna(df.loc[idx, 'temp_owner2_last_name']):
            current_first = df.loc[idx, 'temp_owner2_first_name']
            current_last = df.loc[idx, 'temp_owner2_last_name']
            
            predicted_order, confidence = get_llm_name_order(current_first, current_last, current_type)
            logger.info(f"Owner 2 Analysis - Name: {current_first} {current_last}, Predicted: {predicted_order}, Confidence: {confidence}")
            
            # Only override if:
            # 1. We have sufficient confidence
            # 2. Current type doesn't match predominant type
            # 3. Predicted order matches our predominant type
            if (predicted_order and 
                confidence >= confidence_threshold and 
                current_type != predominant_type and 
                predicted_order == predominant_type):
                
                standardized_count += 1
                logger.info(f"Standardizing Owner 2 - Original: {current_first} {current_last} -> Swapping to match {predominant_type}")
                df.loc[idx, ['temp_owner2_first_name', 'temp_owner2_last_name']] = \
                    df.loc[idx, ['temp_owner2_last_name', 'temp_owner2_first_name']].values
        
        # Update the type to predominant type after standardization
        df.loc[idx, 'temp_name_type'] = predominant_type
    
    logger.info(f"Standardized {standardized_count} names to {predominant_type} format using LLM assistance")
    return df