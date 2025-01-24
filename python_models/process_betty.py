import json
import os.path
import sys
from pathlib import Path
import pandas
import logging

file_path_betty = sys.argv[1]
og_file_path = sys.argv[2]
brand_prefix = sys.argv[3]
# Assuming containsCustomBrandCompany and brand_prefix are already defined
if brand_prefix is None:
    brand_prefix = "BETTY"

path = Path(og_file_path).parts
file_name = path[-1]
parent_folder = "/".join(path[:-1])
output_file = os.path.join(parent_folder,f"{brand_prefix}_{file_name}")
logging.basicConfig(level=logging.INFO, format="%(message)s")
try:
    betty_dt = pandas.read_csv(file_path_betty)

    og_dt = pandas.read_csv(og_file_path)
    # Rename columns with brand prefix
    rename_mapping = {
        "BETTY_UPDATED_ADDRESS_LINE1": brand_prefix + "_UPDATED_ADDRESS_LINE1",
        "BETTY_UPDATED_ADDRESS_LINE2": brand_prefix + "_UPDATED_ADDRESS_LINE2",
        "BETTY_UPDATED_ADDRESS_CITY": brand_prefix + "_UPDATED_ADDRESS_CITY",
        "BETTY_UPDATED_ADDRESS_STATE": brand_prefix + "_UPDATED_ADDRESS_STATE",
        "BETTY_UPDATED_ADDRESS_ZIP": brand_prefix + "_UPDATED_ADDRESS_ZIP",
        # "BETTY_DEMOGRAPHIC_SCORE": brand_prefix + "_DEMOGRAPHIC_SCORE",
        # "BETTY_ROOF_SCORE": brand_prefix + "_ROOF_SCORE",
        "BETTY_UPDATED_O_1_F_N": brand_prefix + "_UPDATED_O_1_F_N",
        "BETTY_UPDATED_O_1_L_N": brand_prefix + "_UPDATED_O_1_L_N",
        "BETTY_UPDATED_BUSINESS_NAME": brand_prefix + "_UPDATED_BUSINESS_NAME",
        "BETTY SCORE": brand_prefix + " SCORE"
    }

    og_dt["BETTY_UPDATED_ADDRESS_LINE1"] = betty_dt["BETTY_UPDATED_ADDRESS_LINE1"]
    og_dt["BETTY_UPDATED_ADDRESS_LINE2"] = betty_dt["BETTY_UPDATED_ADDRESS_LINE2"]
    og_dt["BETTY_UPDATED_ADDRESS_CITY"] = betty_dt["BETTY_UPDATED_ADDRESS_CITY"]
    og_dt["BETTY_UPDATED_ADDRESS_STATE"] = betty_dt["BETTY_UPDATED_ADDRESS_STATE"]
    og_dt["BETTY_UPDATED_ADDRESS_ZIP"] = betty_dt["BETTY_UPDATED_ADDRESS_ZIP"]
    # if "BETTY_DEMOGRAPHIC_SCORE" in betty_dt.columns:
    #     og_dt["BETTY_DEMOGRAPHIC_SCORE"] = betty_dt["BETTY_DEMOGRAPHIC_SCORE"]
    # if "BETTY_ROOF_SCORE" in betty_dt.columns:
    #     og_dt["BETTY_ROOF_SCORE"] = betty_dt["BETTY_ROOF_SCORE"]
    og_dt["BETTY_UPDATED_O_1_F_N"] = betty_dt["BETTY_UPDATED_O_1_F_N"]
    og_dt["BETTY_UPDATED_O_1_L_N"] = betty_dt["BETTY_UPDATED_O_1_L_N"]
    og_dt["BETTY_UPDATED_BUSINESS_NAME"] = betty_dt["BETTY_UPDATED_BUSINESS_NAME"]
    og_dt["BETTY SCORE"] = betty_dt["BETTY SCORE"]
            
    og_dt = og_dt.rename(columns=rename_mapping)

    # Convert "BETTY SCORE" column to integer
    og_dt[f"{brand_prefix} SCORE"] = og_dt[f"{brand_prefix} SCORE"].astype(int)

    # Sort DataFrame by "BETTY SCORE" column
    og_dt = og_dt.sort_values(by=f"{brand_prefix} SCORE", ascending=False)

    # og_dt.astype({"BETTY SCORE": "int"})

    # og_dt = og_dt.sort_values("BETTY SCORE", ascending=False)

    """
    01/11 Commenting out Betty grouping as it's not needed
    min_betty = og_dt['BETTY SCORE'].min()
    max_betty = og_dt['BETTY SCORE'].max()
    bin_edges = [min_betty + i * ((max_betty - min_betty) / 6) for i in range(7)]
    bin_labels = ['BETTY_GROUP_1', 'BETTY_GROUP_2', 'BETTY_GROUP_3', 'BETTY_GROUP_4', 'BETTY_GROUP_5', 'BETTY_GROUP_6']
    og_dt['BETTY GROUPING'] = pandas.cut(og_dt['BETTY SCORE'], bins=bin_edges, labels=bin_labels, include_lowest=True)
    
    cols = og_dt.columns.tolist()
    cols.insert(-2, cols.pop(cols.index('BETTY GROUPING')))
    og_dt = og_dt.reindex(columns=cols)
    """

    og_dt.to_csv(output_file, index=False)
    logging.info({"success":"True","OriginalFilePath": og_file_path,"NewFilePath": output_file})

except Exception as e:
    exc = str(e).replace('"','')
    exc = str(exc).replace("'",'')
    logging.error({"success":"False","OriginalFilePath": og_file_path,"NewFilePath": output_file, "error": "download_failure", "error_details": exc})
