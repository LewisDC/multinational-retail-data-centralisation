import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import tabula as tb
import requests
import json
import boto3

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        
    def read_rds_table(self, table_name):
        try:
            engine = self.db_connector.engine
            with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:    
                return pd.read_sql(f"SELECT * FROM {table_name}", engine).set_index('index')       
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            return None

    def retrieve_pdf_data(link=None):
        if link == None:
            link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        pdf = tb.read_pdf(link, pages='all', output_format="dataframe")
        pdf_df = pdf[0]
        pdf_df.head()
        return pdf_df    
    
    def list_number_of_stores(self, return_number_stores_endpoint, header):
        response = requests.get(return_number_stores_endpoint, headers=header)
        number_stores = response.json()
        number_of_stores = number_stores['number_stores']
        return number_of_stores

    def retrieve_stores_data(self, number_of_stores, endpoint, header):
        data = []
        for store in range(number_of_stores):
            store_url = f'{endpoint}{store}'
            response = requests.get(store_url, headers=header)
            if response.status_code == 200:
                store_data = pd.json_normalize(response.json())
                data.append(store_data)

            df = pd.concat(data).set_index('index')
        print('Data retieval complete.')
        return df          
    
    def extract_from_s3(self, s3_address):
        s3 = boto3.client('s3')
        bucket, key = s3_address.split('//')[1].split('/', 1)
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'])
        return df
    

if __name__=="__main__":
    yaml_file_path = 'db_creds.yaml'

    db_connector = DatabaseConnector(yaml_file_path)
    extractor = DataExtractor(db_connector)
    
    creds = db_connector.read_db_creds(yaml_file_path)
    table_names = db_connector.list_db_tables(db_connector.engine)

    header = {creds['KEY']: creds['VALUE']}
    retrieve_a_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    return_number_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
      
    ### users details
    users_df = DataExtractor(db_connector).read_rds_table(table_names[1])
    cleaned_users_df = DataCleaning.clean_user_data(users_df)
    
    db_connector.upload_to_db(cleaned_users_df, table_name='dim_users')

    ### Card details
    card_details_df = DataExtractor.retrieve_pdf_data()
    cleaned_card_details_df = DataCleaning.clean_card_data(card_details_df)
    db_connector.upload_to_db(cleaned_card_details_df, table_name='dim_card_details')

    ### stores
    number_of_stores = extractor.list_number_of_stores(return_number_stores_endpoint, header)
    stores_df = extractor.retrieve_stores_data(number_of_stores, retrieve_a_store_endpoint, header)

    cleaned_stores_df = DataCleaning.clean_store_data(stores_df)
    db_connector.upload_to_db(cleaned_stores_df, table_name='dim_store_details')

    ### Products
    s3_address = 's3://data-handling-public/products.csv'
    product_data_df = extractor.extract_from_s3(s3_address)

    cleaned_product_data_df = DataCleaning.clean_products_data(product_data_df)
    db_connector.upload_to_db(cleaned_product_data_df, table_name='dim_products')
    
    ### Orders
    orders_data_df = extractor.read_rds_table('orders_table')

    cleaned_orders_data_df = DataCleaning.clean_orders_data(orders_data_df)
    db_connector.upload_to_db(orders_data_df, table_name='orders_table')

    ### Date events
    date_events_data_df = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    
    cleaned_date_events_data_df = DataCleaning.clean_date_events_data(date_events_data_df)
    db_connector.upload_to_db(cleaned_date_events_data_df, table_name='dim_date_times')

