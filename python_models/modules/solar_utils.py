import os
import pandas as pd

class SolarDataProcessor:
    def __init__(self, with_solar_folder, without_solar_folder):
        self.with_solar_folder = with_solar_folder
        self.without_solar_folder = without_solar_folder

    def load_data_from_folder(self, folder_path):
        dataframes = []
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                df = pd.read_csv(os.path.join(folder_path, filename))
                dataframes.append(df)
        return dataframes

    def load_data(self):
        self.with_solar_data = self.load_data_from_folder(self.with_solar_folder)
        self.without_solar_data = self.load_data_from_folder(self.without_solar_folder)

    # def clean_data(self):
    #     self.with_solar_data = [df[df['date_of_death'].isnull()] if 'date_of_death' in df.columns else df for df in self.with_solar_data]
    #     self.without_solar_data = [df[df['date_of_death'].isnull()] if 'date_of_death' in df.columns else df for df in self.without_solar_data]

    def find_common_columns(self):
        common_columns = set(self.with_solar_data[0].columns)
        for df in self.with_solar_data + self.without_solar_data:
            common_columns &= set(df.columns)
        return list(common_columns)

    def concatenate_dataframes(self, common_columns):
        self.with_solar_df = pd.concat([df[common_columns] for df in self.with_solar_data], ignore_index=True)
        self.without_solar_df = pd.concat([df[common_columns] for df in self.without_solar_data], ignore_index=True)

    def drop_columns(self, columns_to_drop):
        self.with_solar_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
        self.without_solar_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    def save_dataframes(self, with_solar_path, without_solar_path):
        self.with_solar_df.to_csv(with_solar_path, index=False)
        self.without_solar_df.to_csv(without_solar_path, index=False)

    def identify_non_numerical_columns(self):
        self.non_numerical_columns_with_solar = self.with_solar_df.select_dtypes(include=['object', 'category']).columns.tolist()
        self.non_numerical_columns_without_solar = self.without_solar_df.select_dtypes(include=['object', 'category']).columns.tolist()
        return self.non_numerical_columns_with_solar, self.non_numerical_columns_without_solar

    def apply_mapping(self, df, column, mapping):
        df[column] = df[column].map(mapping).fillna(0).astype(int)

    def convert_columns(self, non_numerical_columns, mappings):
        for column, mapping in mappings.items():
            self.apply_mapping(self.with_solar_df, column, mapping)
            self.apply_mapping(self.without_solar_df, column, mapping)

    def save_converted_dataframes(self, with_solar_converted_path, without_solar_converted_path):
        self.with_solar_df.to_csv(with_solar_converted_path, index=False)
        self.without_solar_df.to_csv(without_solar_converted_path, index=False)

    def combine_dataframes(self):
        self.with_solar_df['solar_installed'] = 1
        self.without_solar_df['solar_installed'] = 0
        self.combined_df = pd.concat([self.with_solar_df, self.without_solar_df], ignore_index=True)

    def drop_non_numeric_columns(self):
        non_numeric_columns = self.combined_df.select_dtypes(include=['object', 'category']).columns.tolist()
        self.combined_df.drop(columns=non_numeric_columns, inplace=True, errors='ignore')

    def calculate_correlation_matrix(self):
        return self.combined_df.corr()

    def save_correlation_matrix(self, correlation_matrix, correlation_matrix_path):
        correlation_matrix.to_csv(correlation_matrix_path)

    def save_final_dataframes(self, with_solar_cleaned_final_path, without_solar_cleaned_final_path):
        self.with_solar_df.to_csv(with_solar_cleaned_final_path, index=False)
        self.without_solar_df.to_csv(without_solar_cleaned_final_path, index=False)

#for solar_ml.py ->
def get_selected_features():

    correlation_matrix_dir = os.path.dirname(os.path.dirname( __file__ ))
    correlation_matrix_filename = os.path.join("solar_models", "correlation_matrix.csv")
    correlation_matrix_path = os.path.join(correlation_matrix_dir, correlation_matrix_filename)
    correlation_matrix = pd.read_csv(correlation_matrix_path, index_col=0)


    correlations = correlation_matrix['solar_installed'].abs()
    selected_features = correlations[correlations > 0.1].index.tolist()
    
    if 'solar_installed' in selected_features:
        selected_features.remove('solar_installed')
    
    return selected_features