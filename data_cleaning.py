import pandas as pd
from datetime import datetime

class DataCleaning:

    @staticmethod
    def remove_null_values(df):
        # Remove rows with 'NULL' values
        return df[~df.isin(['NULL']).any(axis=1)]

    @staticmethod
    def remove_duplicates(df):
        df = df.drop_duplicates()
        df = df.drop_duplicates(subset=['phone_number'], keep=False)
        df = df.drop_duplicates(subset=['email_address'], keep=False)
        df = df.drop_duplicates(subset=['user_uuid'], keep=False)
        return df

    @staticmethod
    def clean_country_data(df, valid_country_code=None):
        if valid_country_code is None:
            valid_country_code = ('GB', 'DE', 'US')
        
        df['country'] = df['country'].astype('category')
        df['country_code'] = df['country_code'].astype('category')
        df['country_code'] = df['country_code'].replace('GGB', 'GB')
        incorrect_codes = df[~df['country_code'].isin(valid_country_code)]
        return df.drop(incorrect_codes.index)

    @staticmethod
    def convert_dates(df, date_columns=None):
        if date_columns is None:
            date_columns = ['date_of_birth', 'join_date']
        # Convert date columns to datetime objects
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='ignore')
            df[col] = pd.to_datetime(df[col], format='%Y %B %d', errors='ignore')
            df[col] = pd.to_datetime(df[col], format='%B %Y %d', errors='ignore')
            df[col] = pd.to_datetime(df[col], errors='coerce')
        return df

    @staticmethod
    def clean_email_addresses(df):
        # Replace @@ with @ in the "email" column
        df['email_address'] = df['email_address'].str.replace('@@', '@')
        return df

    @staticmethod
    def clean_addresses(df):
        df['address'] = df['address'].str.replace("\n", ' ')
        df['address'] = df['address'].str.title()
        df['address'] = df['address'].str.split().apply(lambda x: ' '.join(x[:-2] + [word.upper() for word in x[-2:]]))
        return df

    @staticmethod
    def clean_phone_numbers(df):
        try:
            df[['phone_number', 'phone_ext']] = df['phone_number'].str.split('x', expand=True)
        except ValueError as e:
            print(f"No extensions found")
        df['phone_number'] = df['phone_number'].str.replace('(0)', '')
        df['phone_number'] = df['phone_number'].str.replace('[^a-zA-Z0-9+]', '', regex=True)
        return df
    
    @staticmethod
    def convert_data_types(df, column_data_types=None):
        if column_data_types == None:
            column_data_types = {
                'first_name': 'string',
                'last_name': 'string',
                'company': 'string',
                'email_address': 'string',
                'address': 'string',
                'phone_number': 'string',
                'user_uuid': 'string'
                }
        for col, data_type in column_data_types.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(data_type)
                except ValueError as e:
                    print(f"Error converting '{col}' to '{data_type}': {e}")
            else:
                print(f"Column '{col}' not found in the DataFrame.")
        return df

    
    @staticmethod
    def clean_user_data(df):
        df = DataCleaning.remove_null_values(df)
        df = DataCleaning.remove_duplicates(df)
        df = DataCleaning.clean_country_data(df)
        df = DataCleaning.clean_email_addresses(df)
        df = DataCleaning.convert_dates(df)
        df = DataCleaning.clean_addresses(df)
        df = DataCleaning.convert_data_types(df)
        df = DataCleaning.clean_phone_numbers(df)
        return df
    
        
        


