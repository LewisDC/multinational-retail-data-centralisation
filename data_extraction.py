from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import boto3
import pandas as pd
import requests
import tabula as tb

class DataExtractor:
    """
    A class for extracting, transforming, and loading (ETL) data from various sources.

    Parameters:
    - `db_connector` (DatabaseConnector): An instance of the DatabaseConnector class for database connections.

    Methods:
    - `read_rds_table(table_name: str) -> pd.DataFrame`: Read data from an RDS table and return it as a pandas DataFrame.

    - `retrieve_pdf_data(pdf_url: str = None) -> pd.DataFrame`: Retrieve data from a PDF file and return it as a pandas DataFrame.

    - `list_number_of_stores(return_number_stores_endpoint: str, header) -> int`: Get the number of stores from an API endpoint.

    - `retrieve_stores_data(number_of_stores: int, endpoint: str, header) -> pd.DataFrame`: Retrieve data from multiple stores and return it as a pandas DataFrame.

    - `extract_from_s3(s3_address: str) -> pd.DataFrame`: Extract data from an S3 bucket and return it as a pandas DataFrame.

    Usage Example:
    ```python
    db_connector = DatabaseConnector("/path/to/db_creds.yaml")
    data_extractor = DataExtractor(db_connector)
    pdf_df = data_extractor.retrieve_pdf_data()
    ```

    Note: This class assumes the existence of the `DatabaseConnector` and `DataCleaning` classes.
    """

    def __init__(self, db_connector : DatabaseConnector):
        self.db_connector = db_connector
        
    def read_rds_table(self, table_name: str) -> pd:
        """Read data from an RDS table and return it as a pandas DataFrame."""

        try:
            # Create a SQLAlchemy engine using the db_connector
            engine = self.db_connector.engine
            with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
                # Execute a SQL query and fetch the data into a pandas DataFrame.
                return pd.read_sql(f"SELECT * FROM {table_name}", engine).set_index('index')       
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            return None

    def retrieve_pdf_data(pdf_url: str=None) -> pd:
        """Retrieve data from a PDF file and return it as a pandas DataFrame."""

        if pdf_url == None:
            pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        pdf = tb.read_pdf(pdf_url, pages='all', output_format="dataframe")
        pdf_df = pdf[0]
        pdf_df.head()
        return pdf_df    
    
    def list_number_of_stores(self, return_number_stores_endpoint: str, header) -> int:
        """Get the number of stores from an API endpoint."""

        response = requests.get(return_number_stores_endpoint, headers=header)
        number_stores = response.json()
        number_of_stores = number_stores['number_stores']
        return number_of_stores

    def retrieve_stores_data(self, number_of_stores: int, endpoint: str, header) -> pd:
        """Retrieve data from multiple stores and return it as a pandas DataFrame."""

        data = []

        for store in range(number_of_stores):
            store_url = f'{endpoint}{store}'
            response = requests.get(store_url, headers=header)

            if response.status_code == 200:
                store_data = pd.json_normalize(response.json())
                data.append(store_data)

            # Concatenate all the data and set the 'index' column as the DataFrame index
            df = pd.concat(data).set_index('index')

        print('Data retieval complete.')
        return df          
    
    def extract_from_s3(self, s3_address: str) -> pd:
        """Extract data from an S3 bucket and return it as a pandas DataFrame."""
        
        s3 = boto3.client('s3')
        # Extract the bucket and key details from the supplied url using the split method
        bucket, key = s3_address.split('//')[1].split('/', 1)
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'])
        return df
    

