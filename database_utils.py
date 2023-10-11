import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    
    def __init__(self, yaml_file_path):
        self.yaml_file_path = yaml_file_path
        # Initialize the database engine
        self.engine = self.init_db_engine(yaml_file_path)
        
    def read_db_creds(self, yaml_file_path):
        try:
            with open(yaml_file_path, 'r') as yaml_file:
                creds = yaml.safe_load(yaml_file)  
            return creds
        except Exception as e:
            print(f"Error reading YAML file: {e}")
            return None

    def init_db_engine(self, creds):
        creds = self.read_db_creds(self.yaml_file_path)

        if creds:
                # Database connection details
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
            except Exception as e:
                print("Failed to initialise the database engine: {e}")
                return None
        else:
            print("Failed to initialize the database engine.")
            return None

    def list_db_tables(self,engine):
        try:
            # Create a SQLAlchemy inspector object
            inspector = inspect(engine)

            # Get a list of all table names in the database
            table_names = inspector.get_table_names()
            
            return table_names
        except Exception as e:
            print(f"Error listing database tables: {e}")
            return None


#db_connector = DatabaseConnector(yaml_file_path)

#tests


