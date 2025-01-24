from sklearn.preprocessing import LabelEncoder

def data_setup(df):
    if 'Inquiry' in df.columns:
        df.Inquiry = df.Inquiry.fillna(False)
        df.Inquiry = df.Inquiry.replace(2,False)
        df.Inquiry = df.Inquiry.replace(1,True)

    df['potential_age'].fillna(df['potential_age'].mode()[0], inplace = True)
    df['dist_btw_site_mail_zip'].fillna(9999, inplace=True)
    
    df['full_name_missing'] = df['owner_full_name'].isna()
    df['num_missing_in_row'] = df.isna().sum(axis=1)
    
    le = LabelEncoder()
    
    
    feature_columns = ['potential_age',
        'BETTY SCORE',  
    'is_business', 'dist_btw_site_mail_zip', 
        'full_name_missing', 'num_missing_in_row']   
    
    if 'Inquiry' in df.columns:
        feature_columns.append('Inquiry')
    updated_df = df[feature_columns].copy()


    for name in updated_df.columns:
        if updated_df[name].dtype == "object":
            le.fit(updated_df[name].unique())
            updated_df[name] = le.transform(updated_df[name])
    return updated_df
