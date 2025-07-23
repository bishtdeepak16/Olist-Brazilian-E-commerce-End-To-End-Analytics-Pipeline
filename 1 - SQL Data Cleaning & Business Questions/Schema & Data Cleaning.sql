-- All Data Syntax


-- ===================================
-- 1. olist_customers_dataset
-- ===================================
CREATE TABLE olist_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(2)
);

-- ===================================
-- 2. olist_orders_dataset
-- ===================================
CREATE TABLE olist_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- ===================================
-- 3. olist_order_items_dataset
-- ===================================
CREATE TABLE olist_order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    PRIMARY KEY (order_id, order_item_id)
);

-- ===================================
-- 4. olist_order_payments_dataset
-- ===================================
CREATE TABLE olist_order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2),
    PRIMARY KEY (order_id, payment_sequential)
);

-- ===================================
-- 5. olist_order_reviews_dataset
-- ===================================
CREATE TABLE olist_order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- ===================================
-- 6. olist_products_dataset
-- ===================================
CREATE TABLE olist_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g DECIMAL(10,2),
    product_length_cm DECIMAL(10,2),
    product_height_cm DECIMAL(10,2),
    product_width_cm DECIMAL(10,2)
);

-- ===================================
-- 7. olist_sellers_dataset
-- ===================================
CREATE TABLE olist_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(2)
);

-- ===================================
-- 8. product_category_name_translation
-- ===================================
CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100)
);

-- ===================================
-- 9. olist_geolocation_dataset
-- ===================================
CREATE TABLE olist_geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat DECIMAL(9,6),
    geolocation_lng DECIMAL(9,6),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(20)
);



-- DATA CLEANING & MODIFICATION

-- This data is of late 2016,2017 and 2018 till September. The whole data of 2016 and September-2018 is incomplete and inconsistent so we will eliminate it and perform analysis on full 2017 and 2018 till august for better results and insights.
-- Deleting From All the tables containing date columns
DELETE FROM olist_orders
WHERE 
  EXTRACT(YEAR FROM order_purchase_timestamp) = 2016
  OR (
    EXTRACT(YEAR FROM order_purchase_timestamp) = 2018 
    AND EXTRACT(MONTH FROM order_purchase_timestamp) in (9,10)
  );


DELETE FROM olist_order_items
WHERE 
  EXTRACT(YEAR FROM shipping_limit_date) = 2016
  OR (
    EXTRACT(YEAR FROM shipping_limit_date) = 2018 
    AND EXTRACT(MONTH FROM shipping_limit_date) in (9,10)
  );

-- Geolocation Table (olist_geolocation)

-- Found many dirty and inconsistent rows in city column which has almost a million rows in total.
-- Built an automated column profiling function to identify data quality issues like non-ASCII chars, digits, symbols,blank values and repeated patterns.
--
CREATE OR REPLACE FUNCTION profile_column_quality_strict(
    table_name TEXT,
    column_name TEXT
)
RETURNS TABLE(issue TEXT, example_value TEXT, count INTEGER) AS
$$
BEGIN
    -- Create a temporary table to collect profiling results
    CREATE TEMP TABLE tmp_result (
        issue TEXT,
        example_value TEXT,
        count INTEGER
    ) ON COMMIT DROP;

    -- 1. Non-ASCII characters (excluding common invisible Unicode)
    EXECUTE format(
        'INSERT INTO tmp_result
         SELECT ''Non-ASCII characters'', t.%1$I::TEXT, COUNT(*)
         FROM %I t
         WHERE t.%1$I ~ ''[^ -~]''
           AND t.%1$I NOT LIKE ''%%'' || chr(160) || ''%%''
           AND t.%1$I NOT LIKE ''%%'' || chr(8203) || ''%%''
           AND t.%1$I NOT LIKE ''%%'' || chr(173) || ''%%''
         GROUP BY t.%1$I
         ORDER BY COUNT(*) DESC
         LIMIT 50;',
        column_name, table_name
    );

    -- 2. Contains digits
    EXECUTE format(
        'INSERT INTO tmp_result
         SELECT ''Contains digits'', t.%1$I::TEXT, COUNT(*)
         FROM %I t
         WHERE t.%1$I ~ ''[0-9]''
         GROUP BY t.%1$I
         ORDER BY COUNT(*) DESC
         LIMIT 50;',
        column_name, table_name
    );

    -- 3. Contains symbols (excluding space, comma, hyphen)
    EXECUTE format(
        'INSERT INTO tmp_result
         SELECT ''Contains symbols'', t.%1$I::TEXT, COUNT(*)
         FROM %I t
         WHERE t.%1$I ~ ''[^a-zA-Z0-9 ,\\-]''
         GROUP BY t.%1$I
         ORDER BY COUNT(*) DESC
         LIMIT 50;',
        column_name, table_name
    );

    -- 4. Too short (1ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œ2 characters)
    EXECUTE format(
        'INSERT INTO tmp_result
         SELECT ''Too short (1-2 chars)'', t.%1$I::TEXT, COUNT(*)
         FROM %I t
         WHERE LENGTH(TRIM(t.%1$I::TEXT)) BETWEEN 1 AND 2
         GROUP BY t.%1$I
         ORDER BY COUNT(*) DESC
         LIMIT 50;',
        column_name, table_name
    );

    -- 5. Null or blank
    EXECUTE format(
        'INSERT INTO tmp_result
         SELECT ''Null or blank'', ''[empty]'', COUNT(*)
         FROM %I t
         WHERE t.%I IS NULL OR TRIM(t.%I::TEXT) = '''';',
        table_name, column_name, column_name
    );

    -- 6. Composite/multi-part locations (slashes, backslashes, double commas)
    EXECUTE format(
        'INSERT INTO tmp_result
         SELECT ''Composite/multi-part location'', t.%1$I::TEXT, COUNT(*)
         FROM %I t
         WHERE t.%1$I LIKE ''%%/%%''
            OR t.%1$I LIKE ''%%\\%%''
            OR t.%1$I LIKE ''%%,%%,%%''
         GROUP BY t.%1$I
         ORDER BY COUNT(*) DESC
         LIMIT 50;',
        column_name, table_name
    );

    -- 7. Repeated words in value (e.g., "rio de janeiro / rio de janeiro")
    EXECUTE format(
        'INSERT INTO tmp_result
         SELECT ''Repeated words in value'', t.%1$I::TEXT, COUNT(*)
         FROM %I t
         WHERE LOWER(t.%1$I::TEXT) ~ ''(\\m\\w+\\M).*\\1''
         GROUP BY t.%1$I
         ORDER BY COUNT(*) DESC
         LIMIT 50;',
        column_name, table_name
    );

    -- Return all profiling results
    RETURN QUERY SELECT * FROM tmp_result;
END;
$$ LANGUAGE plpgsql;



SELECT * FROM profile_column_quality_strict('olist_geolocation', 'geolocation_city');

-- found accented letters. replacing them
Update olist_geolocation
Set geolocation_city = Translate(geolocation_city, 'ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â£ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â§ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â©ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â­ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚ÂºÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â´ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚ÂªÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Âµ', 'aaceiouoaeo')


-- removing digits found in between character strings
Update olist_geolocation
set geolocation_city = regexp_replace(geolocation_city, '[0-9]','', 'g')
where geolocation_city ~ '[0-9]'


-- removing unnecessary symbols from rows
Update olist_geolocation
set geolocation_city = regexp_replace(geolocation_city, '[^a-zA-z0-9\s]', '', 'g')
where geolocation_city ~ '[^a-zA-z0-9\s]'


-- removing extra spaces from rows
Update olist_geolocation
Set geolocation_city = REGEXP_REPLACE(Trim(geolocation_city), '\s+', ' ', 'g')
WHERE geolocation_city ~ '\s{2,}' OR geolocation_city ~ '^\s+' OR geolocation_city ~ '\s+$';


-- Note: Noticed that geolocation_city column contains typos and inconsistent spellings.(e.g., "sao joao do pau dalho" vs "sao joao do pau d'alho")
-- Cleaned using Python (FuzzyWuzzy) by auto-correcting values with >95% similarity.
-- Replaced less frequent variants with more common ones and saved as cleaned dataset.

ALTER TABLE olist_geolocation
ADD COLUMN geo_id SERIAL PRIMARY KEY; -- Added `geo_id` as a unique identifier for each row in the `olist_geolocation_dataset` to safely update specific rows during cleaning, since `geolocation_city` and other columns had duplicate values.

-- Note: Noticed repeated phrases in some rows.
-- Detected using automation by (FuzyyWuzzy) using python
-- Detected rows like 
-- "rio de janeiro rio de janeiro brasil"
-- "rio de janeiro"

UPDATE olist_geolocation
SET geolocation_city = 'rio de janeiro'
where geolocation_city ilike '%rio de janeiro rio de janeiro%'


-- Seller Table (olist_sellers)

-- Seller table also has seller_city so lets go through it by the automation query - profile_column_quality

SELECT * FROM profile_column_quality_strict('olist_sellers', 'seller_city');

-- Replace accented characters with ASCII equivalents
CREATE EXTENSION IF NOT EXISTS unaccent;

UPDATE olist_sellers
SET seller_city = unaccent(seller_city);

-- Removing digits
Update olist_sellers
set seller_city = regexp_replace(seller_city, '[0-9]','', 'g')
where seller_city ~ '[0-9]'


-- Cleaning and standardize the 'seller_city' column by removing any extra location info
-- (e.g., state or country names) that appear after a separator like comma, slash, backslash, or hyphen.
UPDATE olist_sellers
SET seller_city = REGEXP_REPLACE(seller_city, '[,\\/\\-].*$', '', 'g')
WHERE seller_city ~ '[,\\/\\-]';


-- Deleting rows who had inconsistent format such as municipality city instead of city in brazil.
Delete from olist_sellers
Where seller_city in (
    'vendas@creditparts.com.br',
    'bahia',
    'centro',
    'minas gerais',
    'california',
    'holambra',
    'mineiros do tiete',
    'paulo lopes',
    'parai',
    'pirituba',
    'castro pires',
    'neopolis',
    'parana'
);


-- Correction of inconsistent and incomplete rows
Update olist_sellers
set seller_city = 
case when seller_city = 'sp' then 'sao paulo'
when seller_city = 'arraial d''ajuda (porto seguro)' THEN 'porto seguro'
end
where seller_city in ('sp',
    'arraial d''ajuda (porto seguro)')
	
	
-- removing extra spaces from rows
UPDATE olist_sellers
SET seller_city = regexp_replace(trim(seller_city), '\s+', ' ', 'g')
WHERE seller_city ~ '\s{2,}' OR seller_city ~ '^\s' OR seller_city ~ '\s$';


-- Replacing 'Unknown' in NULL values
UPDATE olist_sellers
SET seller_city = 'Unknown'
WHERE seller_city IS NULL
   OR TRIM(seller_city) = '';


-- Note: Noticed that seller_city column also contains typos and inconsistent spellings.(e.g., "sao joao do pau dalho" vs "sao joao do pau d'alho")
-- Cleaned using Python (FuzzyWuzzy) by auto-correcting values with >95% similarity.
-- Replaced less frequent variants with more common ones and saved as cleaned dataset.


-- Reviews Table (olist_order_reviews)

-- it seems that review table has duplicate review_id which is preventing it from being primary key in olist_order_reviews. So in order to investigate duplicate we have to remove primary key from review_id column.

select review_id,
count(*)
from olist_order_reviews
group by 1
having count(*) > 1

-- Dropping duplicate rows from review_id before creating primary key

-- Remove duplicates, keeping the first one
DELETE FROM olist_order_reviews
WHERE ctid NOT IN (
  SELECT MIN(ctid)
  FROM olist_order_reviews
  GROUP BY review_id
);


-- Recreating the primary key constraint after cleaning

alter table olist_order_reviews
add primary key (review_id)


-- Creating dim products table to add product category english translation column

create view olist_dim_products as (
select p.product_id,
product_category_name_english as product_category
from olist_products p
join product_category_name_translation pt on p.product_category_name = pt.product_category_name
)

-- Orders Table (olist_orders_status)
-- Creating New Column to measure delivery days

ALTER TABLE olist_orders
add column delivery_diff_days int;

UPDATE olist_orders
SET delivery_diff_days = CASE
    WHEN order_delivered_customer_date IS NOT NULL THEN
        order_delivered_customer_date::date - order_purchase_timestamp::date
    ELSE NULL
END;


-- Creating Delivery Status(On Time/Late) column 

ALTER TABLE olist_orders
add column delivery_status text;

update olist_orders
set delivery_status = Case when order_delivered_customer_date <= order_estimated_delivery_date then 'On-Time'
when order_delivered_customer_date > order_estimated_delivery_date then 'Late'
else 'N/a' end


-- Creating Dim Calendar Table for time intelligence in power bi

Create view dim_calendar as 
with date_bounds as (
select 
min(order_purchase_timestamp)::date as min_date,
max(order_purchase_timestamp)::date as max_date
from olist_orders
),

dates as (
	select generate_series(
(select min_date from date_bounds),
(select max_date from date_bounds),
interval '1 day')::date as date_key
)

select date_key,
extract(year from date_key)::int as year,
extract(month from date_key)::int as month,
to_char(date_key, 'Month') as month_name,
extract(day from date_key)::int as day_of_month,
to_char(date_key, 'Day') as day_name
from dates
order by date_key




