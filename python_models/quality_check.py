import pandas as pd
import sys
import glob
from sklearn.metrics import confusion_matrix,ConfusionMatrixDisplay
import numpy as np


# Load ALL groundtruth into a DF (i.e. load all the _rwr_appfiles)
# Given a folder, it should iterate in the folder and load all the files into a DF
# then it should run quality check on that DF and the output of that should be simple:
# % lower than 100, greater than 100 for Betty Score
# % Contact vs. do not contact for Betty Predicted
# % accurately figured out for betty score
# % accurately figured out for betty predicted
which_betty = 'BETTY SCORE'
which_predicted = 'BETTY PREDICTED'
file_path=''
for i,arg in enumerate(sys.argv):
    if arg == '--file_path':
        file_path = sys.argv[i+1]
    if arg == '--folder_path':
        folder_path = sys.argv[i+1]
    if arg == '--betty_column':
        which_betty = sys.argv[i+1]
    if arg == '--predicted_column':
        which_predicted = sys.argv[i+1]

total_rows = 0
total_above_0 = 0
total_above_100 = 0
total_inquries = 0
total_above_100_inquiries = 0
total_above_100_customers = 0
total_above_0_customers = 0
total_above_0_inquiries = 0

if file_path:
    df = pd.read_csv(file_path, low_memory=False)
else:
    csv_files = glob.glob(folder_path + "/*.csv")
    # Read each CSV file into DataFrame
    # This creates a list of dataframes
    df_list = (pd.read_csv(file) for file in csv_files)

    # Concatenate all DataFrames
    df   = pd.concat(df_list, ignore_index=True)

total_rows = df.shape[0]
total_above_100 = df[df[which_betty] > 100].count()[which_betty]
total_above_0 = df[df[which_betty] > 0].count()[which_betty]

if 'person_type' in df.columns:
    persontypedf = df[df['person_type'] == 'Customer']
    print(persontypedf.shape)
    total_customers = persontypedf.shape[0]
    print("Total_customers", total_customers)
    total_above_100_customers = persontypedf[persontypedf[which_betty] > 100].count()[which_betty]
    total_above_0_customers = persontypedf[persontypedf[which_betty] > 0].count()[which_betty]
    
if 'Inquiry' in df.columns:
    inquirydf = df[df['Inquiry'] == 1]
    print(inquirydf.shape)
    total_inquries = inquirydf.shape[0]
    print("Total_inquries", total_inquries)
    total_above_100_inquiries = inquirydf[inquirydf[which_betty] > 100].count()[which_betty]
    total_above_0_inquiries = inquirydf[inquirydf[which_betty] > 0].count()[which_betty]
    if which_predicted in df.columns:
        count_correctly_predicted_inquiries = inquirydf[inquirydf[which_predicted] == 'CONTACT'].count()[which_predicted]


print('Total number of rows: ', total_rows)
#print('$$ Savings in this file if ignore rows below 0: ', round(0.60 * (total_rows - total_above_0), 2) )
print('$$ Savings in this file: $', round(0.67 * (total_rows - total_above_100), 2))
print('BETTY - Number of mailers saved in this campaign: ', total_rows - total_above_100)
#print('Percentage of rows at or above 0: ', round((100/total_rows) * total_above_0, 2))
print('BETTY - Percentage of rows at or above 100: ', round((100/total_rows) * total_above_100, 2))
print('BETTY - Percentage of rows below 100: ', round((100/total_rows) * (total_rows-total_above_100), 2))
if which_predicted in df.columns:
    print('PREDICTED -- Percentage of rows predicted as do not mail: ', round(100/total_rows*df.groupby([which_predicted]).size()["DO_NOT_CONTACT"], 2))

if 'Inquiry' in df.columns:
    print('BETTY -- Percentage of inquiries rightly predicted for betty at or above 100: ', round(100/total_inquries * total_above_100_inquiries, 2))
    #print('Percentage of inquiries rightly predicted for betty at or above 0: ', round(100/total_inquries * total_above_0_inquiries, 2))
    df.Inquiry = df.Inquiry.fillna(False)
    df.Inquiry = df.Inquiry.replace(1,True)
    df.Inquiry = df.Inquiry.replace(2,False)
    if which_predicted in df.columns:
        print('PREDICTED - Percentage of inquiries rightly predicted as CONTACT: ', round(100/total_inquries * count_correctly_predicted_inquiries, 2))

if 'person_type' in df.columns:
    print('BETTY -- Percentage of customers rightly predicted for betty at or above 100: ', round(100/total_customers * total_above_100_customers, 2))
    print('Percentage of customers rightly predicted for betty at or above 0: ', round(100/total_customers * total_above_0_customers, 2))
    #y = np.array(df["Inquiry"]).astype('bool')
    #df["Predicted_Betty_Score"] = df["BETTY SCORE"] >=100
    #cm = confusion_matrix(y,df["Predicted_Betty_Score"])
    #disp = ConfusionMatrixDisplay(confusion_matrix=cm,display_labels=["No/Bad","Success"])

    #disp.plot()
    #plt.show()


