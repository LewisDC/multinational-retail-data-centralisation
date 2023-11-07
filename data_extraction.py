import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

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

    def upload_dim_users():
        yaml_file_path = 'db_creds.yaml'
        db_connector = DatabaseConnector(yaml_file_path)
        table_names = db_connector.list_db_tables(db_connector.engine)
        df = DataExtractor(db_connector).read_rds_table(table_names[1])
        print(df.head())
        cleaned_df = DataCleaning.clean_user_data(df)
        table_name = 'dim_users'
        db_connector.upload_to_db(cleaned_df, table_name)