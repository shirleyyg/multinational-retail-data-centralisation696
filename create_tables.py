'''
Once data has been extracted and cleaned, upload_to_db method from  DatabaseConnector class
is used to create tables and store the data in new tables in sales_data database as follows
'''

# # table 'dim_users'
# db_engine = DatabaseConnector().init_db_engine()
# extractor = DataExtractor(db_engine)
# table_data = extractor.read_rds_table('legacy_users')
# a = DataCleaning(table_data)
# updated_table = a.clean_user_data()
# updated_table.head(2)
# # new_engine = DatabaseConnector()
# # new_engine.upload_to_db(updated_table, 'dim_users')


# table 'dim_card_details'
# db_engine2 = DatabaseConnector().init_db_engine()
# extractor2 = DataExtractor(db_engine2)
# new_table = extractor2.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
# cleaner = DataCleaning(new_table)
# cards = cleaner.clean_card_data()
# # cards.head(10)
# # cards.info()
# new_engine = DatabaseConnector()
# new_engine.upload_to_db(cards, 'dim_card_details')


# # table dim_store_details
# db_engine3 = DatabaseConnector().init_db_engine()
# extractor = DataExtractor(db_engine3)
# data = extractor.retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/")
# a = DataCleaning(data)
# stores1 = a.called_clean_store_data()
# # stores.head(451)

# new_engine = DatabaseConnector()
# new_engine.upload_to_db(stores1, 'dim_store_details')
    

# table dim_products
# db_engine = DatabaseConnector().init_db_engine()
# extractor = DataExtractor(db_engine)
# csv_data = extractor.extract_from_s3("s3://data-handling-public/products.csv")
# a = DataCleaning(csv_data)
# updated_table = a.clean_products_data()
# updated_table = a.convert_product_weights()

# new_engine = DatabaseConnector()
# new_engine.upload_to_db(updated_table, 'dim_products')


# # table 'orders_table'
# db_engine = DatabaseConnector()
# conn = DatabaseConnector().init_db_engine()
# ext = DataExtractor(conn)
# ord = ext.read_rds_table('orders_table')
# a = DataCleaning(ord)
# ord_table = a.clean_orders_data()
# # ord_table.head(2)

# new_engine = DatabaseConnector()
# new_engine.upload_to_db(ord_table, 'orders_table')


# table 'dim_date_times'
# db_engine = DatabaseConnector()
# conn = DatabaseConnector().init_db_engine()
# ext = DataExtractor(conn)
# db = ext.read_json_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
# db.head(1000)
# a = DataCleaning(db)
# date_times = a.clean_date_data()
# date_times.head(2)

# new_engine = DatabaseConnector()
# new_engine.upload_to_db(date_times, 'dim_date_times')