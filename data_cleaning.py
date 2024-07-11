'''
DataCleaning class will consist of methods which will perform the cleaning imported data from various data sources.
The methods will clean the imported data, check for NULL values, errors with dates, 
incorrectly typed values and rows filled with the wrong information.
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
        """
        The function `clean_user_data` cleans user data by dropping rows with all missing values,
        converting date of birth and join date to datetime format, validating phone numbers with a regex
        expression, and removing slashes from addresses.
        :return: The `clean_user_data` method is returning the cleaned user data after performing various
        data cleaning operations such as dropping rows with all missing values, converting the
        'date_of_birth', 'join_date' columns to datetime format, validating and cleaning the
        'phone_number' column based on a regex expression, and removing '/' from the 'address' column. The
        cleaned user data is then returned.
        """
        self.user_data.dropna(axis=0, how='all', inplace = True)  
        self.user_data['date_of_birth'] = pd.to_datetime(self.user_data['date_of_birth'], errors='coerce')
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' #Our regular expression to match
        self.user_data.loc[~self.user_data['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan # For every row  where the Phone column does not match our regular expression, replace the value with NaN
        self.user_data['join_date'] = pd.to_datetime(self.user_data['join_date'], errors='coerce')
        self.user_data['address'] = self.user_data['address'].str.replace('/', '')
        return self.user_data

    @staticmethod
    def parse_date(date_str):
        """
        The function `parse_date` attempts to parse a date string using dateparser with exception
        handling.
        
        :param date_str: The function `parse_date(date_str)` is designed to parse a date string using
        the `dateparser` library with exception handling. If the parsing is successful, it returns the
        parsed date object. If an exception occurs during parsing, it returns `pd.NaT` (NaT stands for
        Not
        :return: The function `parse_date` is returning the parsed date if successful, or `pd.NaT` (a
        pandas NaT value representing a missing datetime) if there was an error during parsing.
        """
        try:
            parsed_date = dateparser.parse(date_str)
            return parsed_date
        except Exception as e:
            # print(f"Error parsing date '{date_str}': {e}")
            return pd.NaT
           
    def clean_card_data(self):
        """
        The `clean_card_data` function cleans the user data by removing rows with missing values, dropping
        duplicate records, and converting a date column to datetime format.
        :return: The `clean_card_data` method is returning the cleaned `user_data` DataFrame after
        performing various data cleaning operations such as dropping rows with missing values, dropping
        duplicate records, and converting the 'date_payment_confirmed' column to date values.
        """
        self.user_data.dropna(how='all', inplace = True) #removes rows that contain missing value.
        self.user_data.dropna(axis=0, thresh=3, inplace = True) 
        self.user_data.dropna(axis=1, how='all', inplace = True)
        self.user_data.drop_duplicates() #drop duplicate records 
        # Apply the parse_date method to date_payment_confirmed to convert strings to date values
        self.user_data['date_payment_confirmed'] = self.user_data['date_payment_confirmed'].apply(self.parse_date)
        self.user_data['date_payment_confirmed'] = pd.to_datetime(self.user_data['date_payment_confirmed'], format="ISO8601", errors='coerce')
        return self.user_data
            
    def called_clean_store_data(self):
        """
        The function `clean_store_data` removes rows with all null values, cleans the 'address' column,
        parses dates in the 'opening_date' column, and returns the cleaned data.
        :return: The `clean_store_data` method is returning the cleaned and processed `user_data`
        DataFrame after dropping rows with all null values, removing special characters from the 'address'
        column, parsing dates in the 'opening_date' column, and converting the 'opening_date' column to
        datetime format with ISO8601 standard.
        """
        self.user_data.dropna(how='all', inplace = True) #removes rows whose all columns have null values
        self.user_data['address'] = self.user_data['address'].str.replace('\n', '', regex=False).str.replace('/', '', regex=False)
        # Apply the parse_date method to the 'opening_date' column to parse dates to date values and format
        self.user_data['opening_date'] = self.user_data['opening_date'].apply(self.parse_date)
        self.user_data['opening_date'] = pd.to_datetime(self.user_data['opening_date'], format="ISO8601", errors='coerce')
        return self.user_data
    
    @staticmethod
    def convert_weight(weight):
        """
        The function `convert_weight` converts weight values in various units (kg, g, ml) to a decimal
        value representing weight in kilograms or returns None for invalid values.
        
        :param weight: The `convert_weight` function is designed to convert weight values in different
        units (kg, g, ml) to a decimal value representing weight in kilograms. If the input weight is in
        a valid format, it will be converted accordingly. If the input is not in a recognized format,
        the function will
        :return: The `convert_weight` function is designed to convert weight values in various formats
        (kg, g, ml) to a decimal value representing weight in kilograms. If the input `weight` is a
        string, the function processes it based on different conditions:
        """
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

    def convert_product_weights(self):
        """
        The function `convert_product_weights` converts the weight column in a DataFrame from one unit to
        another and drops the original weight column.
        :return: The `convert_product_weights` method returns a new DataFrame `converted_df` that is a
        copy of the original DataFrame `user_data`, with a new column `product_weight_kg` that contains
        the weights converted to kilograms. The original `weight` column is dropped from the DataFrame
        before returning it.
        """
        converted_df = self.user_data.copy()
        converted_df['product_weight_kg'] = converted_df['weight'].apply(lambda x: self.convert_weight(x))
        converted_df.drop(columns=['weight'], inplace=True)   # Drop original weight column
        return converted_df

    def clean_products_data(self):
        """
        The function `clean_products_data` cleans and processes product data by removing missing values,
        converting data types, and formatting prices.
        :return: The `clean_products_data` method is returning the cleaned `user_data` DataFrame after
        performing various data cleaning operations such as dropping rows with all missing values,
        dropping a specific column, converting a column to datetime format, removing currency symbols
        from a column, converting a column to numeric type, filling missing values with 0, and rounding
        the values in the 'product_price' column to 2 decimal places
        """
        self.user_data.dropna(how='all', inplace = True)
        self.user_data.drop(columns=['Unnamed: 0'], inplace=True)
        self.user_data['date_added'] = pd.to_datetime(self.user_data['date_added'], errors='coerce') #convert object field to date
        self.user_data['product_price'] = self.user_data['product_price'].str.replace('£', '')
        self.user_data['product_price'] = pd.to_numeric(self.user_data['product_price'], errors="coerce")
        self.user_data.fillna(0)
        self.user_data['product_price'] = self.user_data['product_price'].apply(lambda x: round(float(x), 2))
        return self.user_data

    def clean_orders_data(self):
        """
        The function `clean_orders_data` removes the 'level_0' column and any columns with missing values
        from the user_data DataFrame.
        :return: The cleaned orders data after dropping the 'level_0' column and any columns with missing
        values is being returned.
        """
        self.user_data.drop(columns=['level_0'], inplace=True)
        self.user_data.dropna(axis=1, inplace=True) 
        return self.user_data

    def clean_date_data(self):
        """
        The function `clean_date_data` converts date-related columns in a DataFrame to numeric and
        datetime formats with error handling.
        :return: The `clean_date_data` method returns the `self.user_data` DataFrame after cleaning and
        converting the 'month', 'year', 'day', and 'timestamp' columns to numeric and datetime formats
        respectively.
        """
        self.user_data['month'] = pd.to_numeric(self.user_data['month'], errors='coerce')
        self.user_data['year'] = pd.to_numeric(self.user_data['year'], errors='coerce')
        self.user_data['day'] = pd.to_numeric(self.user_data['day'], errors='coerce')
        self.user_data['timestamp'] = pd.to_datetime(self.user_data['timestamp'], format='mixed', errors='coerce').dt.time
        return self.user_data