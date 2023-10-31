import pandas as pd
from datetime import datetime

class DataCleaning:

    def __init__(self, df):
        self.df = df

    def remove_null_values(self):
        # Remove rows with 'NULL' values
        self.df = self.df[~self.df.isin(['NULL']).any(axis=1)]

    def remove_duplicates(self):
        self.df = self.df.drop_duplicates(inplace=True)
        self.df = self.df.drop_duplicates(subset=['phone_number'], keep=False, inplace=True)
        self.df = self.df.drop_duplicates(subset=['email_address'], keep=False, inplace=True)
        self.df = self.df.drop_duplicates(subset=['user_uuid'], keep=False, inplace=True)

    def clean_country_data(self, valid_country_code=None):
        
        if valid_country_code is None:
            valid_country_code = ('GB', 'DE', 'US')
        
        self.df['country'] = self.df['country'].astype('category')
        self.df['country_code'] = self.df['country_code'].astype('category')
        self.df['country_code'] = self.df['country_code'].replace('GGB', 'GB')
        incorrect_codes = self.df[~self.df['country_code'].isin(valid_country_code)]
        self.df = self.df.drop(incorrect_codes.index)
        

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
        for col in date_columns():
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce')

    def clean_email_addresses(self):
        # Replace @@ with @ in the "email" column
        self.df['email_address'] = self.df['email_address'].str.replace('@@', '@')

    def clean_dates(self, date_columns):
        for col in date_columns:
            self.df[col] = self.df[col].apply(self.convert_incorrect_date_formats)

    def clean_addresses():
        pass

    def clean_phone_numbers():
        pass

    def clean_user_data(self):
        self.remove_null_values()
        self.remove_duplicates()
        self.clean_country_data()
        self.clean_email_addresses()
        date_columns = ['date_of_birth', 'join_date']
        self.clean_dates(date_columns)
        self.clean_addresses()
        self.clean_phone_numbers()
        self.convert_dates()
        


