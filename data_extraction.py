import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        
    def read_rds_table(self, table_name):
        try:
            engine = self.db_connector.engine
            with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:    
                df = pd.read_sql(f"SELECT * FROM {table_name}", engine).set_index('index')
                return df
        
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            return None

yaml_file_path = 'db_creds.yaml'
db_connector = DatabaseConnector(yaml_file_path)
table_name = "legacy_users"
user_df = DataExtractor(db_connector).read_rds_table(table_name)
user_df.describe()