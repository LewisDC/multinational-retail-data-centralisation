from datetime import datetime
from dateutil.parser import parse
import pandas as pd

class DataCleaning:
    """
    A class for cleaning and transforming data in a pandas DataFrame.

    Methods:
    - `remove_null_values(df: pd.DataFrame) -> pd.DataFrame`: Remove rows with 'NULL' values.

    - `remove_unique_column_duplicates(df: pd.DataFrame, columns_with_unique_values: list[str]) -> pd.DataFrame`: Remove duplicates from specified columns.

    - `clean_country_data(df: pd.DataFrame, valid_country_code: list[str] = None) -> pd.DataFrame`: Clean country data, convert country code to category, and remove invalid codes.

    - `convert_dates(df: pd.DataFrame, date_columns: list[str]) -> pd.DataFrame`: Convert date columns to datetime objects.

    - `clean_email_addresses(df: pd.DataFrame) -> pd.DataFrame`: Clean email addresses by replacing '@@' with '@'.

    - `clean_addresses(df: pd.DataFrame) -> pd.DataFrame`: Clean addresses by removing unwanted formatting and capitalizing.

    - `clean_phone_numbers(df: pd.DataFrame) -> pd.DataFrame`: Clean phone numbers by extracting extension numbers, removing unwanted characters, and stripping non-alphanumeric characters.

    - `convert_data_types(df: pd.DataFrame, column_data_types: dict = None) -> pd.DataFrame`: Convert specified columns to the specified data types.

    - `convert_product_weights(weight_str: str) -> str`: Convert product weights to kilograms.

    - `clean_user_data(df: pd.DataFrame) -> pd.DataFrame`: Clean user data by converting data types, removing null values, removing duplicates, cleaning country data, cleaning email addresses, converting dates, cleaning addresses, and cleaning phone numbers.

    - `clean_card_data(df: pd.DataFrame) -> pd.DataFrame`: Clean card data by removing duplicates, unwanted characters, unwanted letters, normalizing date columns, and converting numerical columns to integer dtype.

    - `clean_store_data(df: pd.DataFrame) -> pd.DataFrame`: Clean store data by removing unusual data, cleaning country data, removing unwanted characters and letters, converting N/A to NaN, normalizing date columns, and converting specified columns to the specified data types.

    - `clean_products_data(df: pd.DataFrame) -> pd.DataFrame`: Clean products data by renaming columns, correcting spelling, dropping rows with invalid values, cleaning product price and weight columns, normalizing date columns, and converting specified columns to the specified data types.

    - `clean_orders_data(df: pd.DataFrame) -> pd.DataFrame`: Clean orders data by dropping specified columns and renaming columns.

    - `clean_date_events_data(df: pd.DataFrame) -> pd.DataFrame`: Clean date events data by dropping rows with invalid time periods, converting timestamp to time, and converting specified columns to the specified data types.

    Usage Example:
    ```python
    data_cleaner = DataCleaning()
    cleaned_user_data = data_cleaner.clean_user_data(raw_user_data)
    ```

    Note: This class assumes the existence of the pandas library for DataFrame manipulation.
    """

    @staticmethod
    def remove_null_values(df: pd) -> pd:
        # Remove rows with 'NULL' values
        return df[~df.isin(['NULL']).any(axis=1)]

    @staticmethod
    def remove_unique_column_duplicates(df: pd, columns_with_unique_values: list[str]) -> pd:
        # Removes duplicates from the DataFrame and then the columns with unique values.
        # Note: 'keep' is set to False to remove all entries which are duplicates.
        for column in columns_with_unique_values:
            df = df.drop_duplicates(subset=[column], keep=False)       
        return df

    @staticmethod
    def clean_country_data(df: pd, valid_country_code: list[str]=None) -> pd:
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
    def convert_dates(df: pd, date_columns: list[str]) -> pd:
        # Convert date columns to datetime objects
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='ignore')
            df[col] = pd.to_datetime(df[col], format='%Y %B %d', errors='ignore')
            df[col] = pd.to_datetime(df[col], format='%B %Y %d', errors='ignore')
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        return df

    @staticmethod
    def clean_email_addresses(df: pd) -> pd:
        # Replace @@ with @ in the "email" column
        df['email_address'] = df['email_address'].str.replace('@@', '@')
        return df

    @staticmethod
    def clean_addresses(df: pd) -> pd:
        # Removes unwanted formatting
        df['address'] = df['address'].str.replace("\n", ' ')
        # Capitalise the first letter
        df['address'] = df['address'].str.title()
        # Capitalise Zip codes, postal codes (includes city for German addresses)
        df['address'] = df['address'].str.split().apply(lambda x: ' '.join(x[:-2] + [word.upper() for word in x[-2:]]))
        return df

    @staticmethod
    def clean_phone_numbers(df: pd) -> pd:
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
    def convert_data_types(df: pd, column_data_types: dict=None) -> pd:
        
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
    def convert_product_weights(weight_str: str) -> str:
        # Convert the values to string before normalising the data to kilograms, removing unwanted characters and converting them to 'float' dtype 
        weight_str = str(weight_str)
        # Calculate total weight of multipacks
        if 'x' in weight_str:
            weight_str = weight_str.replace('g', '')
            factors = weight_str.split(' x ')
            return float(factors[0]) * float(factors[1]) / 1000
        # Remove misplaced full stop
        elif weight_str.endswith('.'):
            weight_str = weight_str.replace('g .', '')
            return float(weight_str) / 1000
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
        # Identify data that does not fit into the above criteria
        else: 
            print(f'{weight_str} not included')
            return weight_str

    
    @staticmethod
    def clean_user_data(df: pd) -> pd:
        ''' Clean user data by converting data types, removing null values, removing duplicates, cleaning country data, cleaning email addresses, converting dates, cleaning addresses, and cleaning phone numbers.'''

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
        columns_with_unique_values = ['email_address', 'phone_number', 'user_uuid']
        df = DataCleaning.remove_unique_column_duplicates(df, columns_with_unique_values)
        df = DataCleaning.clean_country_data(df)
        df = DataCleaning.clean_email_addresses(df)
        date_columns = ['date_of_birth', 'join_date']
        df = DataCleaning.convert_dates(df, date_columns)
        df = DataCleaning.clean_addresses(df)        
        df = DataCleaning.clean_phone_numbers(df)
        return df
    
    @staticmethod
    def clean_card_data(df: pd) -> pd:
        ''' Clean card data by removing duplicates, unwanted characters, unwanted letters, normalizing date columns, and converting numerical columns to integer dtype.'''

        # Remove columns with repeated index headers
        df = df.drop_duplicates(keep=False)
        # Remove unwanted characters
        df['card_number'] = df['card_number'].str.replace('?', '')
        # Remove unwanted letters
        df = df[~df['card_number'].str.contains('[a-zA-Z?]', na=False)]
        # Normalise date columns
        df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(parse).dt.date
        # Remove rows with missing values
        df = df.dropna()
        # Convert numerical column to integer dtype
        df['card_number'] = df['card_number'].astype('int64')
        return df

    @staticmethod
    def clean_store_data(df: pd) -> pd:
        ''' Clean store data by removing unusual data, cleaning country data, removing unwanted characters and letters, converting N/A to NaN, normalizing date columns, and converting specified columns to the specified data types.'''

        # Remove unusual data column
        df = df.drop(columns=['lat'])
        # 
        df = DataCleaning.clean_country_data(df)
        # Remove unwanted characters and letters
        df['address'] = df['address'].str.replace("\n", ', ')
        df['continent'] = df['continent'].str.replace('ee', '')
        df['staff_numbers'] = df['staff_numbers'].replace('[a-zA-Z]', '', regex=True)
        # Convert N/A value to NaN to make it compatible with Float64 dtype
        df['longitude'] = df['longitude'].str.replace('N/A', 'NaN')
        # Normalise date columns
        df['opening_date'] = df['opening_date'].apply(parse).dt.date
        
        column_data_types = {
            'longitude': 'float64',
            'latitude': 'float64',
            'staff_numbers': 'int16'
        }
        df = DataCleaning.convert_data_types(df, column_data_types)
        return df
    
    @staticmethod
    def clean_products_data(df: pd) -> pd:
        ''' Clean products data by renaming columns, correcting spelling, dropping rows with invalid values, cleaning product price and weight columns, normalizing date columns, and converting specified columns to the specified data types.'''

        df.rename(columns={'Unnamed: 0': 'index'}, inplace=True)
        correct_spelling = lambda x: 'Still_available' if x == 'Still_avaliable' else x
        df['removed'] = df['removed'].apply(correct_spelling)
        valid_removed = ('Still_available', 'Removed')
        df.drop(df[~df['removed'].isin(valid_removed)].index, inplace=True)
        df['product_price'] = df['product_price'].str.replace('£', '')
        df['weight'] = df['weight'].apply(DataCleaning.convert_product_weights)
        df.rename(columns={'weight': 'weight_in_kg'}, inplace=True)
        df['date_added'] = df['date_added'].apply(parse).dt.date
        column_data_types = {
            'product_price': 'float64',
            'weight_in_kg': 'float64'            
        }
        df = DataCleaning.convert_data_types(df, column_data_types)
        df['product_price'] = df['product_price'].round(2)
        return df
    
    @staticmethod
    def clean_orders_data(df: pd) -> pd:
        ''' Clean orders data by dropping specified columns and renaming columns.'''

        df.drop(columns=['first_name', 'last_name', '1'], inplace=True)
        df.rename(columns={'level_0': 'index'}, inplace=True)
        return df
    
    @staticmethod
    def clean_date_events_data(df: pd) -> pd:
        ''' Clean date events data by dropping rows with invalid time periods, converting timestamp to time, and converting specified columns to the specified data types.'''

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
        