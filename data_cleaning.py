'''
Class with methods to clean data from each of the data sources

DataCleaning class will consist of methods which will perform the cleaning of the user data.
The methods will clean the imported data, check for NULL values, errors with dates, 
incorrectly typed values and rows filled with the wrong information.

Once extracted and cleaned, we will use the upload_to_db method from  DatabaseConnector class
to store the data in new tables in sales_data database
'''

import pandas as pd
import numpy as np
import dateparser
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning:
    def __init__(self, userdata):
        self.user_data = userdata

    def clean_user_data(self):
        self.user_data.dropna(axis=0, how='all', inplace = True)  
        self.user_data['date_of_birth'] = pd.to_datetime(self.user_data['date_of_birth'], errors='coerce')
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' #Our regular expression to match
        self.user_data.loc[~self.user_data['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan # For every row  where the Phone column does not match our regular expression, replace the value with NaN
        self.user_data['join_date'] = pd.to_datetime(self.user_data['join_date'], errors='coerce')
        self.user_data['address'] = self.user_data['address'].str.replace('/', '')
        # self.user_data.Age.astype('int64')
        return self.user_data
    
    def clean_card_data(self):
        self.user_data.dropna(how='all', inplace = True) #removes rows that contain missing value.
        self.user_data.dropna(axis=0, thresh=4) # drops rows with less than 4 non-missing values
        self.user_data.drop_duplicates() #drop duplicate records 
        return self.user_data
    
    def called_clean_store_data(self):
        self.user_data.dropna(how='all', inplace = True) #removes rows whose all columns have null values
        self.user_data['address'] = self.user_data['address'].str.replace('\n', '', regex=False).str.replace('/', '', regex=False)
        # Define a function to parse dates using dateparser with exception handling
        def parse_date(date_str):
            try:
                parsed_date = dateparser.parse(date_str)
                return parsed_date
            except Exception as e:
                # print(f"Error parsing date '{date_str}': {e}")
                return pd.NaT
        # Apply the function to the 'opening_date' column to parse dates
        self.user_data['opening_date'] = self.user_data['opening_date'].apply(parse_date)
        self.user_data['opening_date'] = pd.to_datetime(self.user_data['opening_date'], format="ISO8601", errors='coerce')
        return self.user_data
    
    def convert_product_weights(self):
        converted_df = self.user_data.copy()
        '''
        Function to convert "weight" column values (kg,g,ml) to a decimal value representing weight in kg.
        or return None for invalid values 
        '''
        def convert_weight(weight):
            if isinstance(weight, str):
                weight_value = str(weight).lower().strip()  # Convert to lowercase and remove whitespace

                if 'x' in weight_value:  # Check if there is value in format A x B
                    value = weight_value.split('x')
                    try:
                        weight = float(value[0]) * float(value[1])
                        return round(weight / 1000, 2)  # Convert to kg assuming weight is in g
                    except (ValueError, IndexError):
                        return None
                elif weight_value.endswith('kg'):
                    weight = weight_value[:-2]
                    return round(float(weight), 2)
                elif weight_value.endswith('g'):
                    return round(float(weight_value[:-1]) / 1000, 2)
                elif weight_value.endswith('ml'):
                    return round(float(weight_value[:-2]) / 1000, 2)
                else:
                    return None
            elif isinstance(weight, float):
                return round(weight, 2)  # If it's already a float, just return it
            else:
                return pd.to_numeric(weight_value) 
        
        converted_df['product_weight_kg'] = converted_df['weight'].apply(lambda x: convert_weight(x))
        converted_df.drop(columns=['weight'], inplace=True)   # Drop original weight column
        return converted_df


    def clean_products_data(self):
        self.user_data.dropna(how='all', inplace = True)
        self.user_data.drop(columns=['Unnamed: 0'], inplace=True)
        self.user_data['date_added'] = pd.to_datetime(self.user_data['date_added'], errors='coerce') #convert object field to date
        self.user_data['product_price'] = self.user_data['product_price'].str.replace('£', '')
        self.user_data['product_price'] = pd.to_numeric(self.user_data['product_price'], errors="coerce")
        self.user_data.fillna(0)
        self.user_data['product_price'] = self.user_data['product_price'].apply(lambda x: round(float(x), 2))
        return self.user_data

    def clean_orders_data(self):
        self.user_data.drop(columns=['level_0'], inplace=True)
        self.user_data.dropna(axis=1, inplace=True) 
        return self.user_data

    def clean_date_data(self):
        self.user_data['month'] = pd.to_numeric(self.user_data['month'], errors='coerce')
        self.user_data['year'] = pd.to_numeric(self.user_data['year'], errors='coerce')
        self.user_data['day'] = pd.to_numeric(self.user_data['day'], errors='coerce')
        self.user_data['timestamp'] = pd.to_datetime(self.user_data['timestamp'], format='mixed', errors='coerce').dt.time
        return self.user_data