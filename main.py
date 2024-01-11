from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
import pandas as pd

if __name__ == '__main__':
    yaml_file_path = 'db_creds.yaml'

    db_connector = DatabaseConnector(yaml_file_path)
    extractor = DataExtractor(db_connector)

    creds = db_connector.read_db_creds(yaml_file_path)
    table_names = db_connector.list_db_tables(db_connector.engine)

    pdf_url = creds['PDF_URL']
    header = {creds['KEY']: creds['VALUE']}
    retrieve_a_store_endpoint = creds['STORE_ENDPOINT']
    return_number_stores_endpoint = creds['NO_OF_STORES']

    ### Users details ETL
    users_df = extractor.read_rds_table(table_names[1])

    cleaned_users_df = DataCleaning.clean_user_data(users_df)
    db_connector.upload_to_db(cleaned_users_df, table_name='dim_users')

    ### Card details ETL
    card_details_df = extractor.retrieve_pdf_data(pdf_url)

    cleaned_card_details_df = DataCleaning.clean_card_data(card_details_df)
    db_connector.upload_to_db(cleaned_card_details_df, table_name='dim_card_details')

    ### Stores data ETL
    number_of_stores = extractor.list_number_of_stores(return_number_stores_endpoint, header)
    stores_df = extractor.retrieve_stores_data(number_of_stores, retrieve_a_store_endpoint, header)

    cleaned_stores_df = DataCleaning.clean_store_data(stores_df)
    db_connector.upload_to_db(cleaned_stores_df, table_name='dim_store_details')

    ### Products data ETL
    s3_address = 's3://data-handling-public/products.csv'
    product_data_df = extractor.extract_from_s3(s3_address)

    cleaned_product_data_df = DataCleaning.clean_products_data(product_data_df)
    db_connector.upload_to_db(cleaned_product_data_df, table_name='dim_products')

    ### Orders data ETL
    orders_data_df = extractor.read_rds_table('orders_table')

    cleaned_orders_data_df = DataCleaning.clean_orders_data(orders_data_df)
    db_connector.upload_to_db(orders_data_df, table_name='orders_table')

    ### Date events ETL
    date_events_data_df = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

    cleaned_date_events_data_df = DataCleaning.clean_date_events_data(date_events_data_df)
    db_connector.upload_to_db(cleaned_date_events_data_df, table_name='dim_date_times')