import pandas as pd
import re
from datetime import datetime
from dateutil.parser import parse

class DataCleaning:

    @staticmethod
    def remove_null_values(df):
        # Remove rows with 'NULL' values
        return df[~df.isin(['NULL']).any(axis=1)]

    @staticmethod
    def remove_duplicates(df):
        # Removes duplicates from the DataFrame and then the columns with unique values.
        # Note: 'keep' is set to False to remove all entries which are duplicates.
        df = df.drop_duplicates()
        df = df.drop_duplicates(subset=['phone_number'], keep=False)
        df = df.drop_duplicates(subset=['email_address'], keep=False)
        df = df.drop_duplicates(subset=['user_uuid'], keep=False)
        return df

    @staticmethod
    def clean_country_data(df, valid_country_code=None):
        #  Establish valid country codes
        if valid_country_code is None:
            valid_country_code = ('GB', 'DE', 'US')
        # The 'country_code' column is converted to categorical dtypes.
        df['country_code'] = df['country_code'].astype('category')
        # The 'country_code' column is checked for invalid values, such as 'GGB', and replaced with 'GB'.
        df['country_code'] = df['country_code'].replace('GGB', 'GB')
        incorrect_codes = df[~df['country_code'].isin(valid_country_code)]
        # Rows with country codes not in the valid_country_code list are removed.
        return df.drop(incorrect_codes.index)

    @staticmethod
    def convert_dates(df, date_columns):
        # Convert date columns to datetime objects
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='ignore')
            df[col] = pd.to_datetime(df[col], format='%Y %B %d', errors='ignore')
            df[col] = pd.to_datetime(df[col], format='%B %Y %d', errors='ignore')
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        return df

    @staticmethod
    def clean_email_addresses(df):
        # Replace @@ with @ in the "email" column
        df['email_address'] = df['email_address'].str.replace('@@', '@')
        return df

    @staticmethod
    def clean_addresses(df):
        # Removes unwanted formatting
        df['address'] = df['address'].str.replace("\n", ' ')
        # Capitalise the first letter
        df['address'] = df['address'].str.title()
        # Capitalise Zip codes, postal codes (includes city for German addresses)
        df['address'] = df['address'].str.split().apply(lambda x: ' '.join(x[:-2] + [word.upper() for word in x[-2:]]))
        return df

    @staticmethod
    def clean_phone_numbers(df):
        # Extracts extension numbers present in some US phone numbers and relocates them to a new column
        try:
            df[['phone_number', 'phone_ext']] = df['phone_number'].str.split('x', expand=True)
        except ValueError as e:
            print(f"No extensions found")
        # Removes the bracketed zero '(0)' from numbers with the country code present
        df['phone_number'] = df['phone_number'].str.replace('(0)', '')
        # Strips all non-alphanumeric characters except for '+'
        df['phone_number'] = df['phone_number'].str.replace('[^a-zA-Z0-9+]', '', regex=True)
        return df
    
    @staticmethod
    def convert_data_types(df, column_data_types=None):
        #Convert alphanumeric columns to 'string' dtype for consistency, simplicity and compatibility
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
    def convert_product_weights(weight_str):
        # Convert the values to string before normalising the data to kilograms, removing unwanted characters and converting them to 'float' dtype 
        weight_str = str(weight_str)
        # Calculate total weight of multipacks
        if 'x' in weight_str:
            weight_str = weight_str.replace('g', '')
            factors = weight_str.split(' x ')
            return float(factors[0]) * float(factors[1]) / 1000
        # Remove misplaced full stop
        elif weight_str.endswith('.'):
            weight_str = weight_str.replace(' .', '')
            return weight_str
        # kg to kg
        elif weight_str.endswith('kg'):
            weight_str = weight_str.replace('kg', '')
            return float(weight_str)
        # g to kg
        elif weight_str.endswith('g'):
            weight_str = weight_str.replace('g', '')
            return float(weight_str) / 1000
        # ml to kg
        elif weight_str.endswith('ml'):
            weight_str = weight_str.replace('ml', '')
            return float(weight_str) / 1000
        # Oz to kg
        elif weight_str.endswith('oz'):
            weight_str = weight_str.replace('oz', '')
            return float(weight_str) * 0.0283495
        else: 
            print(f'{weight_str} not included')
            return weight_str

    
    @staticmethod
    def clean_user_data(df):
        column_data_types = {
            'first_name': 'string',
            'last_name': 'string',
            'company': 'string',
            'email_address': 'string',
            'address': 'string',
            'country': 'category',
            'country_code': 'category',
            'phone_number': 'string',
            'user_uuid': 'string'
            }
        df = DataCleaning.convert_data_types(df, column_data_types)
        df = DataCleaning.remove_null_values(df)
        df = DataCleaning.remove_duplicates(df)
        df = DataCleaning.clean_country_data(df)
        df = DataCleaning.clean_email_addresses(df)
        date_columns = ['date_of_birth', 'join_date']
        df = DataCleaning.convert_dates(df, date_columns)
        df = DataCleaning.clean_addresses(df)        
        df = DataCleaning.clean_phone_numbers(df)
        return df
    
    @staticmethod
    def clean_card_data(df): 
        df = df.drop_duplicates(keep=False)
        df['card_number'] = df['card_number'].str.replace('?', '')
        df = df[~df['card_number'].str.contains('[a-zA-Z?]', na=False)]
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(parse).dt.date
        df = df.dropna()
        df['card_number'] = df['card_number'].astype('int64')
        return df

    @staticmethod
    def clean_store_data(df):
        df = df.drop(columns=['lat'])
        df = DataCleaning.clean_country_data(df)
        df['address'] = df['address'].str.replace("\n", ', ')
        df['continent'] = df['continent'].str.replace('ee', '')
        df['longitude'] = df['longitude'].str.replace('N/A', 'NaN')
        df['opening_date'] = df['opening_date'].apply(parse).dt.date
        df['staff_numbers'] = df['staff_numbers'].replace('[a-zA-Z]', '', regex=True)
        column_data_types = {
            'longitude': 'float64',
            'latitude': 'float64',
            'staff_numbers': 'int16'
        }
        df = DataCleaning.convert_data_types(df, column_data_types)
        return df
    
    @staticmethod
    def clean_products_data(df):
        df.rename(columns={'Unnamed: 0': 'index'}, inplace=True)
        correct_spelling = lambda x: 'Still_available' if x == 'Still_avaliable' else x
        df['removed'] = df['removed'].apply(correct_spelling)
        valid_removed = ('Still_available', 'Removed')
        df.drop(df[~df['removed'].isin(valid_removed)].index, inplace=True)
        df['product_price'] = df['product_price'].str.replace('£', '')
        df['weight'] = df['weight'].apply(DataCleaning.convert_product_weights)
        df.rename(columns={'weight': 'weight(kg)', 'product_price': 'product_price(£)'}, inplace=True)
        df['date_added'] = df['date_added'].apply(parse).dt.date
        column_data_types = {
            'product_price(£)': 'float64',
            'weight(kg)': 'float64'            
        }
        df = DataCleaning.convert_data_types(df, column_data_types)
        return df
    
    @staticmethod
    def clean_orders_data(df):
        df.drop(columns=['first_name', 'last_name', '1'], inplace=True)
        df.rename(columns={'level_0': 'index'}, inplace=True)
        return df
    
    @staticmethod
    def clean_date_events_data(df):
        valid_time_periods = ['Evening', 'Midday', 'Morning', 'Late_Hours']
        df.drop(df[~df['time_period'].isin(valid_time_periods)].index, inplace=True)
        df['timestamp'] = df['timestamp'].apply(parse).dt.time
        column_data_types = {
            'time_period': 'category',
            'month': 'int8',
            'year': 'int16',
            'day': 'int8'
        }
        df = DataCleaning.convert_data_types(df, column_data_types)
        return df
        


