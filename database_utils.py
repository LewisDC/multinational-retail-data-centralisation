import psycopg2
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    """
    A utility class for managing connections to relational databases using SQLAlchemy.

    Parameters:
    - `yaml_file_path` (str): The path to the YAML file containing database credentials.

    Methods:
    - `read_db_creds(yaml_file_path)`: Read and parse database credentials from a YAML file.

    - `init_db_engine(creds)`: Initialize and return the SQLAlchemy engine based on the provided credentials.

    - `list_db_tables(engine)`: Get a list of all table names in the connected database.

    - `upload_to_db(df, table_name)`: Upload a pandas DataFrame to a specified database table.

    Attributes:
    - `yaml_file_path` (str): The path to the YAML file containing database credentials.
    
    - `engine` (sqlalchemy.engine.Engine): The SQLAlchemy engine for database connections.

    Usage Example:
    ```python
    yaml_file_path = "/path/to/db_creds.yaml"
    db_connector = DatabaseConnector(yaml_file_path)
    table_names = db_connector.list_db_tables(db_connector.engine)
    ```
    """
    
    def __init__(self, yaml_file_path):
        self.yaml_file_path = yaml_file_path
        # Initialize the database engine
        self.engine = self.init_db_engine(yaml_file_path)
        
    def read_db_creds(self, yaml_file_path):
        """Read and parse database credentials from a YAML file."""

        try:
            # Open yaml file with database credentials
            with open(yaml_file_path, 'r') as yaml_file:
                creds = yaml.safe_load(yaml_file)  
            return creds
        except yaml.YAMLError as e:
            # Return an error message if it fails
            print(f"Error reading YAML file: {e}")
            return None

    def init_db_engine(self, creds):
        """Initialize and return the SQLAlchemy engine based on the provided credentials."""

        # Read database credentials from YAML
        creds = self.read_db_creds(self.yaml_file_path)

        if creds:
                # RDS database connection details
            DATABASE_TYPE = 'postgresql'
            DBAPI = 'psycopg2'
            ENDPOINT = creds['RDS_HOST']
            USER = creds['RDS_USER']
            PASSWORD = creds['RDS_PASSWORD']
            PORT = creds['RDS_PORT']
            DATABASE = creds['RDS_DATABASE']

            try:
                # Initialize and return the SQLAlchemy engine
                engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
                return engine
            except psycopg2.OperationalError as e:
                # Return an error message if it fails
                print(f"Failed to initialise the database engine: {e}")
                return None
        else:
            print("Failed to initialize the database engine.")
            return None

    def list_db_tables(self, engine):
        """Get a list of all table names in the connected database."""

        try:
            # Create a SQLAlchemy inspector object
            inspector = inspect(engine)

            # Get a list of all table names in the database
            table_names = inspector.get_table_names()
            
            return table_names
        except Exception as e:
            print(f"Error listing database tables: {e}")
            return None
        
    def upload_to_db(self, df, table_name):
        """Upload a pandas DataFrame to a specified database table."""

        # Assign credentials to local variable
        creds = self.read_db_creds(self.yaml_file_path)
        
        if creds:
                # Local database connection details
            DATABASE_TYPE = 'postgresql'
            DBAPI = 'psycopg2'
            HOST = creds['HOST']
            USER = creds['USER']
            PASSWORD = creds['PASSWORD']
            DATABASE = creds['DATABASE']
            PORT = creds['PORT']
            try:
                # Create a database engine using SQLAlchemy
                engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
                # insert the pandas data frame into the Postgresql table
                df.to_sql(table_name, engine, if_exists='replace', index=False)
                # Print confirmation message
                print(f"Data has been uploaded to the '{table_name}' table successfully.")
        
            except psycopg2.OperationalError as e:
                # Return an error message if it fails
                print(f"An error occurred: {str(e)}")





