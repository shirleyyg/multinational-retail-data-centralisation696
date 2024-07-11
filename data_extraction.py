'''
DataExtractor class contains methods that will be fit to extract data from various data sources.
These sources will include CSV files, an API and an S3 bucket.
'''
import pandas as pd
import tabula
import requests
import json
import boto3
import yaml
from sqlalchemy import create_engine
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self, database_connector):
        self.db_conn = database_connector
        self.headers = self.load_apikey()

    def read_rds_table(self, table_name):
        """
        The function reads data from a specified table in an RDS database using pandas.
        
        :param table_name: The `table_name` parameter in the `read_rds_table` function is a string that
        represents the name of the table in the database that you want to read data from
        :return: The function `read_rds_table` returns the data from the specified table in the database
        as a pandas DataFrame.
        """
        with self.db_conn.connect() as conn:
            data = pd.read_sql_table(table_name, conn)
            return data
        
    def retrieve_pdf_data(self, pdf_link):
        """
        The function `retrieve_pdf_data` reads data from a PDF file located at the specified link using
        the Tabula library and returns it as a Pandas DataFrame.
        
        :param pdf_link: The `pdf_link` parameter is a string that represents the URL link to a PDF file
        that you want to retrieve data from. In the provided code snippet, the function
        `retrieve_pdf_data` takes this `pdf_link` as input, reads the PDF file using tabula library, and
        returns the
        :return: The function `retrieve_pdf_data` returns a Pandas DataFrame containing the data
        extracted from the PDF file specified by the `pdf_link` parameter.
        """
        #pdf_path = pdf_link # example "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        pdf = tabula.read_pdf(pdf_link, pages='all', stream=True)
        df = pd.concat(pdf, ignore_index=True)
        return df
    
    def read_json_data(self, filepath):
        """
        The function reads JSON data from a specified file path and returns it as a pandas DataFrame.
        
        :param filepath: The `filepath` parameter in the `read_json_data` function is a string that
        represents the path to the JSON file that you want to read and convert into a pandas DataFrame.
        This file can be a local file path on your system or a URL pointing to a JSON file hosted online
        :return: A DataFrame containing the JSON data from the specified file path is being returned.
        """
        # response = requests.get("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
        response = requests.get(filepath)
        data = json.loads(response.text)
        df = pd.DataFrame(data)
        return df

    @staticmethod
    def load_apikey(self):
        """
        The function `load_apikey` reads and returns the content of a YAML file containing an API token.
        :return: The `load_apikey` function is returning the content of the `api_token.yaml` file
        located at `/Users/sg/DS/MRDC/` after loading it using `yaml.safe_load`.
        """
        with open('/Users/sg/DS/MRDC/api_token.yaml', 'r') as file:
            return yaml.safe_load(file)

    def list_number_of_stores(self, endpoint):
        """
        The function retrieves the number of stores from a specified API endpoint and returns the
        data in a pandas DataFrame.
        
        :param endpoint: The `endpoint` parameter in the `list_number_of_stores` function is the URL of an
        API endpoint that returns information about the number of stores. In the provided code snippet, the
        function sends a GET request to the specified endpoint, retrieves the JSON response, and then
        converts the data into a
        :return: The function `list_number_of_stores` returns a pandas DataFrame containing the data
        retrieved from the specified endpoint after making a GET request with the provided headers.
        """
        # endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        endpoint = endpoint
        response = requests.get(endpoint, headers=self.headers)
        data = response.json()
        num_of_stores = pd.DataFrame([data])
        return num_of_stores
    
    def retrieve_stores_data(self, endpoint):
        """
        The function retrieves data for 451 stores from a specified API endpoint and returns the data in
        a pandas DataFrame.
        
        :param endpoint: The `endpoint` parameter in the `retrieve_stores_data` function is the base URL
        for the API endpoint from which store details are retrieved. In the provided code snippet, the
        `endpoint` is set to a specific URL "https://aqj7u5id95.execute-api.eu-west-
        :return: The function `retrieve_stores_data` is returning a pandas DataFrame containing the data
        retrieved from the specified endpoint for each store number in the range from 0 to 450.
        """
        endpoint = endpoint #"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
        responses = []
        for store_number in range(451):
            store_endpoint = f"{endpoint}{store_number}"
            response = requests.get(store_endpoint, headers=self.headers)
            responses.append(response.json())
        stores = pd.DataFrame(responses)
        # stores = pd.concat([pd.DataFrame(response) for response in responses], ignore_index=True)
        return stores
    
    def extract_from_s3(self, address):
        """
        The function `extract_from_s3` downloads a file from an S3 bucket and reads its content into a
        Pandas DataFrame.
        
        :param address: The `extract_from_s3` function you provided is designed to extract data from an
        S3 bucket and return it as a Pandas DataFrame. The `address` parameter is expected to be a
        string representing the S3 path to the file you want to extract. For example, if you have a
        :return: A Pandas DataFrame containing the data extracted from the specified S3 address is being
        returned. If an error occurs during the extraction process, None is returned along with an error
        message printed to the console.
        """
        address = address
        s3 = boto3.client('s3')
        # address = "s3://data-handling-public/products.csv"
        try:
            filepath = address.split('/')
            bucket_name = filepath[2]
            key = '/'.join(filepath[3:])
                    
                    # Download file from S3
            response = s3.get_object(Bucket=bucket_name, Key=key)
                    
                    # Read CSV data into Pandas DataFrame
            df = pd.read_csv(response['Body']) 
            return df
        
        except Exception as e:
            print("Error extracting data from S3:", e)
            return None