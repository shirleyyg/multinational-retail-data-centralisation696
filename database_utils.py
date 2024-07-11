'''
Class used to connect with and upload data to a SQL database using SQLAlchemy
'''
import yaml
from sqlalchemy import create_engine
from sqlalchemy import text

class DatabaseConnector:
    def __init__(self, file):
        """
        The function initializes a database connection using credentials stored in a YAML file.
        
        :param file: The `file` parameter in the `__init__` method is used to specify the path to the
        YAML file containing database credentials. This path will be assigned to the `yaml_file_path`
        attribute of the class instance
        """
        # self.yaml_file_path = '/Users/sg/DS/MRDC/db_creds.yaml'
        self.yaml_file_path = file
        self.credentials = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        """
        The function reads database credentials from a YAML file.
        :return: The `read_db_creds` method reads the contents of a YAML file located at
        `self.yaml_file_path` and returns the parsed data using `yaml.safe_load` method.
        """
        with open(self.yaml_file_path, 'r') as file:
            return yaml.safe_load(file)
        
    def init_db_engine(self):
        """
        The function `init_db_engine` creates a PostgreSQL database engine using the provided credentials.
        :return: The `init_db_engine` method returns a SQLAlchemy engine object that is connected to a
        PostgreSQL database using the provided credentials. The engine is configured with the specified
        isolation level of 'AUTOCOMMIT'.
        """
        return create_engine(f"postgresql+psycopg2://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}", isolation_level='AUTOCOMMIT')

    def list_db_tables(self):
        """
        This function retrieves and prints the names of tables in a PostgreSQL database schema named
        'public'.
        """
        with self.engine.connect() as conn:
            fetch_tables = text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            rows = conn.execute(fetch_tables).fetchall()
            for row in rows:
                print(row)

    def upload_to_db(self, data_frame, table_name):
        """
        The function `upload_to_db` uploads a pandas DataFrame to a specified table in a database using
        SQLAlchemy.
        
        :param data_frame: The `data_frame` parameter in the `upload_to_db` function is typically a pandas
        DataFrame that contains the data you want to upload to a database table. It is the data that you
        want to insert or replace in the specified table in the database
        :param table_name: Table name is a parameter that specifies the name of the table where the data
        from the DataFrame will be uploaded in the database
        """
        # yaml_file_path2 = '/Users/sg/DS/MRDC/localdb_creds.yaml'
        with self.engine.connect() as conn:
            data_frame.to_sql(table_name, conn, if_exists='replace')