-- Milestone 4 - Quering the data

-- 1> How many stores does the business have and in which countries?
select country_code AS country, count(store_code) AS total_no_stores 
from public.dim_store_details
where country_code in ('GB','DE','US') and address is not null
group by country_code


-- 2. Which locations have the most stores?

select locality, count(store_code) AS total_no_stores 
from public.dim_store_details
where country_code in ('GB','DE','US') and address is not null
group by locality
order by total_no_stores desc
LIMIT 7


-- 3. Which months produced the largest amount of sales?

SELECT ROUND(CAST(SUM(e.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales, d.month
FROM public.orders_table o
JOIN public.dim_products e on o.product_code = e.product_code
JOIN public.dim_date_times d on d.date_uuid = o.date_uuid
GROUP BY d.month
ORDER BY total_sales DESC
LIMIT 6


-- 4. How many sales are coming from online?
SELECT 
    COUNT(o.product_code) AS numbers_of_sales, 
    SUM(o.product_quantity) AS product_quantity_count, 
    CASE 
        WHEN o.store_code = 'WEB-1388012W' THEN 'Web' 
        ELSE 'Offline' 
    END AS location
FROM 
	public.orders_table o
JOIN 
	public.dim_products e ON o.product_code = e.product_code
JOIN
	public.dim_store_details s on s.store_code = o.store_code
GROUP BY location;


-- 5. What percentage of sales comes from each type of store? which stores generates most revenue top 5
SELECT 
    s.store_type,
    ROUND(CAST(SUM(e.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales,
    ROUND(CAST(SUM(e.product_price * o.product_quantity) AS NUMERIC) * 100.0 / CAST(SUM(SUM(e.product_price * o.product_quantity)) OVER() AS NUMERIC), 2) AS percentage_total
FROM 
    public.orders_table o
JOIN 
    public.dim_products e ON o.product_code = e.product_code
JOIN
    public.dim_store_details s ON s.store_code = o.store_code
GROUP BY s.store_type
ORDER BY total_sales DESC;


-- 6. Which month in each year produced the highest cost of sales?
SELECT ROUND(CAST(SUM(e.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales, d.year, d.month
FROM public.orders_table o
JOIN public.dim_products e on o.product_code = e.product_code
JOIN public.dim_date_times d on d.date_uuid = o.date_uuid
GROUP BY d.year, d.month
ORDER BY total_sales DESC
LIMIT 10


-- 7. What is our staff head count?
SELECT 
    SUM(staff_numbers) AS total_staff_numbers, country_code
FROM 
	public.dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC


-- 8. Which German store type is selling the most?
SELECT 
		ROUND(CAST(SUM(e.product_price * o.product_quantity) AS NUMERIC), 2) AS total_sales, 
		s.store_type, 
		s.country_code
FROM public.orders_table o
JOIN public.dim_products e on o.product_code = e.product_code
JOIN public.dim_store_details s ON s.store_code = o.store_code
WHERE s.country_code = 'DE'
GROUP BY s.country_code, s.store_type
ORDER BY total_sales


-- 9. How quickly is the company making sales?
WITH timestamps AS (
    SELECT 
        year,
        TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') as datetime
    FROM 
        public.dim_date_times
    ORDER BY 
        datetime
),
sales_timestamps AS (
    SELECT
        year,
        datetime,
        LEAD(datetime, 1) OVER (ORDER BY datetime DESC) as time_difference
    FROM 
        timestamps
),
average_diff AS (
	SELECT
		year,
		AVG(datetime - time_difference) AS avg_time
	FROM
		sales_timestamps
	GROUP BY
		year
)
SELECT 
    year,
	CONCAT(' "hours": ' || EXTRACT(HOUR FROM avg_time) || ', ' || '"minutes": ' || EXTRACT(MINUTE FROM avg_time) || ', ' || '"seconds": ' || EXTRACT(SECOND FROM avg_time)) AS actual_time_taken
FROM 
    average_diff
GROUP BY 
    	year, avg_time
ORDER BY 
    	avg_time desc
LIMIT 5
