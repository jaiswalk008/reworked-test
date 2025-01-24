import pandas as pd
import sys
import logging
import time
from utils import read_file, remove_duplicate_header_rows

logging.basicConfig(level=logging.INFO, format="%(message)s")
file_format = ''
file_path = ''
output_path = ''
fileIdGenerate='false'
try :
    for i,arg in enumerate(sys.argv):
        if arg == '--file_format':
            file_format = sys.argv[i+1]
        if arg == '--file_path':
            file_path = sys.argv[i+1]
        if arg == '--output_path':
            output_path = sys.argv[i+1]
        if arg == '--file_id_generate':
            fileIdGenerate = sys.argv[i+1]
    if file_path == '':
            raise Exception('File path is missing')
    
    if output_path == '':
        raise Exception('Output path is missing')
    
    if file_format not in ['xls', 'xlsx', 'csv']:
        raise Exception('File format is not supported')
    
    df = read_file(file_path)       
    df.dropna(how="all", inplace=True)
    df = remove_duplicate_header_rows(df)
    df.columns = df.columns.str.replace("'"," ")
    if(fileIdGenerate=='true'):
        timestamp = int(time.time()) 
        df['RW_ID'] = df.index.map(lambda idx: (idx+1) * timestamp)
    df.to_csv(output_path, index=False, header=True)
    if df.shape[0]==0:
        raise Exception('Empty file')
    
    logging.info({"success":"True","OriginalFilePath": file_path,"NewFilePath": output_path, "mapped_cols":{'row_count' : len(df.index)}})

except Exception as e:
    exc = str(e).replace('"','')
    exc = str(exc).replace("'",'')
    logging.error({"success":"False","OriginalFilePath": file_path,"NewFilePath": output_path, "error": "initial processing failure", "error_details": exc})