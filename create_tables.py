'''
This code snippet is performing a series of data extraction, cleaning, and uploading operations to the
PostgreSQL database, sales_data. This python file can be executed to create the SQL tables.
'''
import pandas as pd
import tabula
import numpy as np
import requests
import json
import dateparser
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from dateutil.parser import parse

engine = DatabaseConnector('/Users/sg/DS/MRDC/db_creds.yaml').init_db_engine()
extractor = DataExtractor(engine)
sales_db = DatabaseConnector('/Users/sg/DS/MRDC/localdb_creds.yaml')

# table 'dim_users'
df = extractor.read_rds_table('legacy_users')
cleaner = DataCleaning(df)
updated_table = cleaner.clean_user_data()
sales_db.upload_to_db(updated_table, 'dim_users')

# table 'dim_card_details'
pdf = extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
cleaner = DataCleaning(pdf)
cards = cleaner.clean_card_data()
sales_db.upload_to_db(cards, 'dim_card_details')

# table dim_store_details
data = extractor.retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/")
cleaner = DataCleaning(data)
stores = cleaner.called_clean_store_data()
sales_db.upload_to_db(stores, 'dim_store_details')
    
# table dim_products
csv_data = extractor.extract_from_s3("s3://data-handling-public/products.csv")
cleaner = DataCleaning(csv_data)
updated_table = cleaner.clean_products_data()
updated_table = cleaner.convert_product_weights()
sales_db.upload_to_db(updated_table, 'dim_products')

# table 'orders_table'
ord = extractor.read_rds_table('orders_table')
cleaner = DataCleaning(ord)
ord_table = cleaner.clean_orders_data()
sales_db.upload_to_db(ord_table, 'orders_table')

# table 'dim_date_times'
json_data = extractor.read_json_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
cleaner = DataCleaning(json_data)
date_times = cleaner.clean_date_data()
date_times.head(2)
sales_db.upload_to_db(date_times, 'dim_date_times')