import pandas as pd
from datetime import datetime

class DataCleaning:

    def __init__(self, df):
        self.df = df

    def remove_null_values(self):
        # Remove rows with 'NULL' values
        self.df = self.df[~self.df.isin(['NULL']).any(axis=1)]        

    def convert_dates(self, date_columns):
        # Convert date columns to datetime objects
        for col in date_columns:
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce')

    def correct_data_types(self, column_mapping):
        # Correct data types of specific columns
        for col, data_type in column_mapping.items():
            self.df[col] = self.df[col].astype(data_type, errors='ignore')

    def remove_incorrect_rows(self, condition):
        # Remove rows based on a specified condition
        self.df = self.df[~condition]

    def clean_user_data(self):
        # Define data cleaning operations
        self.remove_null_values()
        date_columns = ['birth_date', 'registration_date']
        self.convert_dates(date_columns)
        column_mapping = {'age': int}
        self.correct_data_types(column_mapping)
        condition = (self.df['age'] < 0)  # Example: Remove rows with negative age
        self.remove_incorrect_rows(condition)