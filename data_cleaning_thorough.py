import pandas as pd
from datetime import datetime

class DataCleaning:

    def __init__(self, df):
        self.df = df

    def remove_null_values(self):
        # Remove rows with 'NULL' values
        null_values = self.df[self.df.isin(['NULL']).any(axis=1)]
        self.df = self.df.drop(null_values.index)

    def remove_duplicates(self):
        self.df.drop_duplicates(inplace=True)
        self.df.drop_duplicates(subset=['phone_number'], keep=False, inplace=True)
        self.df.drop_duplicates(subset=['email_address'], keep=False, inplace=True)
        self.df.drop_duplicates(subset=['user_uuid'], keep=False, inplace=True)
        return self.df

    def clean_country_data(self, valid_country_code=None):
        '''
        Clean and validate country data in the DataFrame.

        This method performs cleaning and validation of country-related data columns
        in the given DataFrame.

        Parameters:
        df (DataFrame): The DataFrame containing country data to be cleaned.
        valid_country_code (iterable, optional): A list of valid country codes.
            Default is None, which uses the predefined list ('GB', 'DE', 'US').

        Returns:
        DataFrame: A cleaned DataFrame with invalid country data removed.

        Description:
        - The 'country' and 'country_code' columns are converted to categorical data types.
        - The 'country_code' column is checked for invalid values, such as 'GGB', and replaced with 'GB'.
        - Rows with country codes not in the valid_country_code list are removed.

        Note:
        The original DataFrame is not modified. The cleaned DataFrame is returned as the result.

        Example:
        df = data_cleaner.clean_country_data(df, valid_country_code=['GB', 'DE'])
        '''

        if valid_country_code is None:
            valid_country_code = ('GB', 'DE', 'US')
        
        self.df['country'] = self.df['country'].astype('category')
        self.df['country_code'] = self.df['country_code'].astype('category')
        self.df['country_code'] = self.df['country_code'].replace('GGB', 'GB')
        incorrect_codes = self.df[~self.df['country_code'].isin(valid_country_code)]
        cleaned_df = self.df.drop(incorrect_codes.index)
        return cleaned_df


    def convert_to_yyyymmdd(self, date_str):
        try:
            # Attempt to parse the date using different date formats
            date = datetime.strptime(date_str, '%Y %B %d')
        except ValueError:
            try:
                date = datetime.strptime(date_str, '%B %Y %d')
            except ValueError:
                try:
                    date = datetime.strptime(date_str, '%Y/%m/%d')
                except ValueError:
                    # If none of the formats match, return None or raise an exception
                    return None

        # Convert the date to 'YYYY-MM-DD' format
        yyyymmdd = date.strftime('%Y-%m-%d')
        return yyyymmdd
    
    def convert_incorrect_date_formats(self, column_name):
        try:    
            condition = ~self.df[column_name].str.match(r'\d{4}-\d{2}-\d{2}')
            self.df.loc[condition, column_name] = self.df.loc[condition, column_name].apply(self.convert_to_yyyymmdd)
            return self.df
        except Exception as e:
            print(f"Error converting date column {column_name}: {e}")
            return None

    def convert_dates(self, date_columns):
        # Convert date columns to datetime objects
        for col, date_format in date_columns.items():
            self.df[col] = pd.to_datetime(self.df[col], format=date_format, errors='coerce')

    def correct_email_addresses(self):
        # Replace @@ with @ in the "email" column
        self.df['email_address'] = self.df['email_address'].str.replace('@@', '@')
        # Validate emails (not needed in this instance):
            # email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+'
            # valid_email_df = df[df['email_address'].str.contains(email_pattern, na=False)]
            # invalid_email_df = df[~df['email_address'].str.contains(email_pattern, na=False)]

    def remove_incorrect_rows(self, condition):
        # Remove rows based on a specified condition
        self.df = self.df[~condition]

    def clean_user_data(self):
        self.df = self.clean_country_data()
        self.correct_email_addresses()
        date_columns = ['date_of_birth', 'join_date']
        for col in date_columns:    
            self.df[col] = self.df[col].apply(self.convert_incorrect_date_formats)    
        for col in date_columns:
            self.df[col] = self.df[col].apply(self.convert_dates)