-- Milestone 3

-- Primary Keys

-- public.dim_users
SELECT user_uuid, COUNT(*) -- to check for duplicate and null values and delete it
FROM public.dim_users
GROUP BY user_uuid
HAVING COUNT(*) > 1;

-- DELETE FROM public.dim_users
-- WHERE user_uuid IS NULL;

ALTER TABLE public.dim_users
ADD PRIMARY KEY (user_uuid);

-- public.dim_products
SELECT product_code, COUNT(*)
FROM public.dim_products
GROUP BY product_code
HAVING COUNT(*) > 1;

-- DELETE FROM public.dim_products
-- WHERE product_code IS NULL;

ALTER TABLE public.dim_products
ADD PRIMARY KEY (product_code);

-- public.dim_store_details
SELECT store_code, COUNT(*)
FROM public.dim_store_details
GROUP BY store_code
HAVING COUNT(*) > 1;

-- DELETE FROM public.dim_store_details
-- WHERE store_code = 'NULL';

ALTER TABLE public.dim_store_details
ADD PRIMARY KEY (store_code);

-- public.dim_date_times
SELECT date_uuid, COUNT(*)
FROM public.dim_date_times
GROUP BY date_uuid
HAVING COUNT(*) > 1;

-- DELETE FROM public.dim_date_times
-- WHERE date_uuid IS NULL;

ALTER TABLE public.dim_date_times
ADD PRIMARY KEY (date_uuid);

-- public.dim_card_details
SELECT card_number, COUNT(*)
FROM public.dim_card_details
GROUP BY card_number
HAVING COUNT(*) > 1;

SELECT * FROM public.dim_card_details
WHERE card_number = 'NULL';

-- delete FROM public.dim_card_details
-- WHERE card_number = 'NULL';

ALTER TABLE public.dim_card_details
ADD PRIMARY KEY (card_number);



--- ADD FOREIGN KEYS to orders_table


ALTER TABLE public.orders_table
ADD CONSTRAINT fk_users
FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid)
ON DELETE CASCADE
ON UPDATE CASCADE;


ALTER TABLE public.orders_table
ADD CONSTRAINT fk_products
FOREIGN KEY (product_code) REFERENCES dim_products(product_code)
ON DELETE CASCADE
ON UPDATE CASCADE;


ALTER TABLE public.orders_table
ADD CONSTRAINT fk_stores
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code)
ON DELETE CASCADE
ON UPDATE CASCADE;


ALTER TABLE public.orders_table
ADD CONSTRAINT fk_date
FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid)
ON DELETE CASCADE
ON UPDATE CASCADE;


ALTER TABLE public.orders_table
ADD CONSTRAINT fk_cards
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number)
ON DELETE CASCADE
ON UPDATE CASCADE;



-- Cleaning tables for null values and invalid data types

-- updating dim_card_details with the missing card numbers found in the order table

-- SELECT DISTINCT from public.dim_card_details
-- SELECT COUNT(*) from public.dim_card_details
	
-- SELECT DISTINCT o.card_number 
-- FROM orders_table o
-- LEFT JOIN public.dim_card_details d
-- ON o.card_number = d.card_number
-- WHERE d.card_number IS NULL


-- INSERT INTO dim_card_details (card_number, expiry_date, card_provider, card_number_expiry_date)
-- SELECT DISTINCT o.card_number, NULL, NULL, NULL
-- FROM orders_table o
-- LEFT JOIN dim_card_details d
-- ON o.card_number = d.card_number
-- WHERE d.card_number IS NULL;



--- Cleaning Store table invalid data values 
-- select count(*) FROM public.dim_store_details

-- SELECT store_code, address
-- FROM public.dim_store_details
-- WHERE store_code LIKE 'WEB%';

-- SELECT store_code, address, opening_date FROM public.dim_store_details
-- where address = 'NA';

-- UPDATE public.dim_store_details
-- SET address = NULL
-- where address = 'NA';


-- SELECT opening_date FROM public.dim_store_details
-- WHERE (opening_date IS NULL OR opening_date::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$');


-- SELECT staff_numbers FROM public.dim_store_details
-- WHERE staff_numbers ~ '[a-zA-Z]';

-- UPDATE public.dim_store_details
-- 	SET staff_numbers = NULL WHERE staff_numbers ~ '[a-zA-Z]';

-- UPDATE public.dim_store_details
-- 	SET latitude = NULL WHERE latitude ~ '[a-zA-Z]';

-- UPDATE public.dim_store_details
-- 	SET longitude = NULL WHERE longitude ~ '[a-zA-Z]';


-- ALTER TABLE public.dim_products
--   	ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
--   	ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
--   	-- ALTER COLUMN EAN TYPE VARCHAR(25) USING EAN::VARCHAR(25), -- ERROR:  column "ean" does not exist
-- 	ALTER COLUMN "EAN" TYPE VARCHAR(25) USING "EAN"::VARCHAR(25);
--   	ALTER COLUMN product_code TYPE VARCHAR(15) USING product_code::VARCHAR(15),
--   	ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
-- 	ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
-- 	ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL,
--   	ALTER COLUMN weight_class TYPE VARCHAR(15) USING weight_class::VARCHAR(15);



-- ALTER TABLE public.dim_products 
-- RENAME COLUMN removed TO still_available;

-- UPDATE public.dim_products 
-- SET still_available = FALSE
-- WHERE still_available != 'true';
	
-- SELECT product_code, still_available FROM public.dim_products
-- 	WHERE still_available != 'true';

-- ALTER TABLE public.dim_products 
-- RENAME COLUMN product_weight_kg TO weight;
	
-- ALTER TABLE public.dim_products
-- ADD COLUMN weight_class VARCHAR(20);

-- UPDATE public.dim_products
-- SET weight_class = CASE
--     WHEN product_weight_kg < 2 THEN 'Light'
--     WHEN product_weight_kg >= 2 AND product_weight_kg < 40 THEN 'Mid_Sized'
--     WHEN product_weight_kg >= 40 AND product_weight_kg < 140 THEN 'Heavy'
--     ELSE 'Truck_Required'
--   END;


-- -- select * from public.dim_products where index = 788

-- -- delete from public.dim_products where index = 788

-- -- cleaning uuid table

-- -- Update rows with invalid UUIDs and set them to NULL

-- select * from public.dim_products
-- 	WHERE uuid IS NOT NULL
-- 	AND uuid !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';


-- UPDATE public.dim_products
-- SET uuid = NULL
-- WHERE uuid IS NOT NULL
-- AND uuid !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
