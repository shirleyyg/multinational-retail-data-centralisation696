'''
Class used to connect with and upload data to the database
'''
import yaml
from sqlalchemy import create_engine
from sqlalchemy import text

class DatabaseConnector:
    def __init__(self):
        self.yaml_file_path = '/Users/sg/DS/MRDC/db_creds.yaml'
        self.credentials = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        with open(self.yaml_file_path, 'r') as file:
            return yaml.safe_load(file)
        
    def init_db_engine(self):
        return create_engine(f"postgresql+psycopg2://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}", isolation_level='AUTOCOMMIT')

    def list_db_tables(self):
        with self.engine.connect() as conn:
            fetch_tables = text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            rows = conn.execute(fetch_tables).fetchall()
            for row in rows:
                print(row)

    def upload_to_db(self, data_frame, table_name):
        yaml_file_path2 = '/Users/sg/DS/MRDC/localdb_creds.yaml'

        with open(yaml_file_path2, 'r') as file:
            credentials2 = yaml.safe_load(file)
        
        engine = create_engine(f"postgresql+psycopg2://{credentials2['RDS_USER']}:{credentials2['RDS_PASSWORD']}@{credentials2['RDS_HOST']}:{credentials2['RDS_PORT']}/{credentials2['RDS_DATABASE']}")

        with engine.connect() as conn:
            data_frame.to_sql(table_name, conn, if_exists='replace')