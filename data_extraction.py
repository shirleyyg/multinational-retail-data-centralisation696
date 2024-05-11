'''
This class will work as a utility class, it contains methods 
that help extract data from different data sources.
The methods contained will be fit to extract data from a particular data source, 
these sources will include CSV files, an API and an S3 bucket.
'''
import pandas as pd
import tabula
import requests
import json
import boto3
from sqlalchemy import create_engine
from database_utils import DatabaseConnector

# db_engine = DatabaseConnector()
# database_connector = db_engine.init_db_engine()
# # tables = db_engine.list_db_tables()

class DataExtractor:
    def __init__(self, database_connector):
        self.db_conn = database_connector
        self.headers = {
            "Content-Type": "application/json",
            "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
            }

    def read_rds_table(self, table_name):
        with self.db_conn.connect() as conn:
            data = pd.read_sql_table(table_name, conn)
            return data
        
    def retrieve_pdf_data(self, pdf_link):
        #pdf_path = pdf_link # example "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        df = tabula.read_pdf(pdf_link, stream=True)
        return df[0]
    
    def read_json_data(self, filepath):
        # df = pd.read_json("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
        # df = pd.read_json(filepath)
        # response = requests.get("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
        response = requests.get(filepath)
        data = json.loads(response.text)
        df = pd.DataFrame(data)
        return df

    def list_number_of_stores(self, endpoint, headers):
        endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        headers = headers
        response = requests.get(endpoint, headers=self.headers)
        data = response.json()
        # print(data)
        num_of_stores = pd.DataFrame([data])
        return num_of_stores
    
    def retrieve_stores_data(self, endpoint):
        # stores = store_endpoint
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