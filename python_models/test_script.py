import glob
import pandas as pd
import logging
import os
import ml
import numpy as np
import modules.rules_engine as rules_engine
import pandas as pd
import sys
from sklearn.metrics import confusion_matrix,ConfusionMatrixDisplay
import numpy as np

python_scripts_path = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(python_scripts_path, "training_files") 

csv_files = glob.glob(data_path + "/*.csv")

# Read each CSV file into DataFrame
# This creates a list of dataframes
df_list = (pd.read_csv(file, low_memory = False) for file in csv_files)

# Concatenate all DataFrames
df   = pd.concat(df_list, ignore_index=True)
logging.basicConfig(level=logging.INFO, format="%(message)s")

try:
    predicted_result = ml.process_ml(df)
        
    total_number_of_true_inquiries = predicted_result[predicted_result['Inquiry'] == True].shape[0]
    predicted_result.Predicted = predicted_result.Predicted.replace("CONTACT", True)
    predicted_result.Predicted = predicted_result.Predicted.replace("DO_NOT_CONTACT", False)
    predictedPercentage,percentageEleminated = ml.gather_print_results(total_number_of_true_inquiries, predicted_result.Predicted, pd.DataFrame(predicted_result, columns = ["Inquiry"]), "RandomForest")
    thresholdPrdictedPercentage = 76
    thresholdPercentageEleminated = 51
    if predictedPercentage < thresholdPrdictedPercentage or percentageEleminated < thresholdPercentageEleminated:
        raise Exception(f"Expected prediction percentage is above {thresholdPrdictedPercentage} but we got {predictedPercentage} Expected percentage eleminated is above {thresholdPercentageEleminated} but we got  {percentageEleminated}")
except Exception as e:
    logging.error(f"Error in test script(): {e}")
    #exit(1)


df_new = df.drop('BETTY SCORE', axis=1)
industry_profile = {"marketing_campaign":["mail"], "property_type":{"land":["medium","large","small"]}}
pseudo_ml_result, count = rules_engine.compute_betty(df_new, industry_profile)
print(pseudo_ml_result.shape)
total_rows = df.shape[0]
which_betty = "BETTY SCORE"
total_above_100 = pseudo_ml_result[pseudo_ml_result[which_betty] > 100].count()[which_betty]
total_above_0 = pseudo_ml_result[pseudo_ml_result[which_betty] > 0].count()[which_betty]

if 'Inquiry' in pseudo_ml_result.columns:
    inquirydf = pseudo_ml_result[pseudo_ml_result['Inquiry'] == 1]
    total_inquries = inquirydf.shape[0]
    print("Total_inquries", total_inquries)
    total_above_100_inquiries = inquirydf[inquirydf[which_betty] > 100].count()[which_betty]
    total_above_0_inquiries = inquirydf[inquirydf[which_betty] > 0].count()[which_betty]


print('Total number of rows: ', total_rows)
print('Percentage of rows below 100: ', round((100/total_rows) * (total_rows-total_above_100), 2))
if 'Inquiry' in pseudo_ml_result.columns:
    print('Percentage of inquiries rightly predicted for betty at or above 100: ', round(100/total_inquries * total_above_100_inquiries, 2))
    



