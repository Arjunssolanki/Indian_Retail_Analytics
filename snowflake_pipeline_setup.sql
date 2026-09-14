CREATE OR REPLACE STORAGE INTEGRATION s3_indian_retail_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://indian-retail-analytics-landing-zone/landing/')
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxxxxxxxx:role/Snowflake_Storage_Integration_Rol';

DESCRIBE INTEGRATION s3_indian_retail_integration;

-- 1. Create Core Database Infrastructure
CREATE OR REPLACE DATABASE indian_retail_db;

CREATE OR REPLACE SCHEMA indian_retail_db.bronze;

CREATE OR REPLACE SCHEMA indian_retail_db.silver;

CREATE OR REPLACE SCHEMA indian_retail_db.gold;

-- 2. Create the External Stage pointing to your S3 Landing folder
CREATE
OR
REPLACE
    STAGE indian_retail_db.bronze.s3_landing_stage STORAGE_INTEGRATION = s3_indian_retail_integration URL = 's3://indian-retail-analytics-landing-zone/landing/';

-- 3. Create the Bronze Landing Table (Using VARIANT to ingest raw files dynamically)
CREATE
OR
REPLACE
TABLE indian_retail_db.bronze.stg_raw_orders (
    raw_data VARIANT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) DATA_RETENTION_TIME_IN_DAYS = 30;
-- Enables 30 Days of Time Travel

-- 1. Build the Snowpipe Ingestion Engine
CREATE OR REPLACE PIPE indian_retail_db.bronze.s3_to_bronze_pipe
AUTO_INGEST = TRUE
AS
COPY INTO indian_retail_db.bronze.stg_raw_orders (raw_data)
FROM @indian_retail_db.bronze.s3_landing_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- 2. Create the Change Data Capture (CDC) Stream
CREATE
OR
REPLACE
    STREAM indian_retail_db.bronze.orders_cdc_stream ON
TABLE indian_retail_db.bronze.stg_raw_orders;

USE DATABASE INDIAN_RETAIL_DB;

USE SCHEMA BRONZE;

SHOW PIPES;

USE DATABASE INDIAN_RETAIL_DB;

USE SCHEMA BRONZE;

-- Manually pull the files currently in your S3 bucket into the Bronze table
COPY INTO indian_retail_db.bronze.stg_raw_orders (raw_data)
FROM (
        SELECT OBJECT_CONSTRUCT (*)
        FROM
            @indian_retail_db.bronze.s3_landing_stage
    ) FILE_FORMAT = (
        TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1
    );

CREATE OR REPLACE PIPE indian_retail_db.bronze.s3_to_bronze_pipe
AUTO_INGEST = TRUE
AS
COPY INTO indian_retail_db.bronze.stg_raw_orders (raw_data)
FROM (
  SELECT OBJECT_CONSTRUCT(*) 
  FROM @indian_retail_db.bronze.s3_landing_stage
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

SELECT *
FROM INDIAN_RETAIL_DB.BRONZE.Streams.CUSTOMERS_CDC_STREAM
LIMIT 5;

USE DATABASE INDIAN_RETAIL_DB;

USE SCHEMA BRONZE;

CREATE
OR
REPLACE
TABLE stg_raw_sales (raw_data VARIANT) DATA_RETENTION_TIME_IN_DAYS = 30;

CREATE
OR
REPLACE
TABLE stg_raw_customers (raw_data VARIANT) DATA_RETENTION_TIME_IN_DAYS = 30;

CREATE
OR
REPLACE
TABLE stg_raw_products (raw_data VARIANT) DATA_RETENTION_TIME_IN_DAYS = 30;
-- 2. Create Separate Streams for CDC
CREATE OR REPLACE STREAM sales_cdc_stream ON TABLE stg_raw_sales;

CREATE
OR
REPLACE
    STREAM customers_cdc_stream ON
TABLE stg_raw_customers;

CREATE
OR
REPLACE
    STREAM products_cdc_stream ON
TABLE stg_raw_products;

-- 3. Create Separate Dedicated Pipes for Automated Loading
CREATE OR REPLACE PIPE pipe_sales AUTO_INGEST = TRUE AS
COPY INTO stg_raw_sales FROM (SELECT OBJECT_CONSTRUCT(*) FROM @s3_landing_stage)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
PATTERN = '.*sales.*\.csv';

CREATE OR REPLACE PIPE pipe_customers AUTO_INGEST = TRUE AS
COPY INTO stg_raw_customers FROM (SELECT OBJECT_CONSTRUCT(*) FROM @s3_landing_stage)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
PATTERN = '.*customers.*\.csv';

CREATE OR REPLACE PIPE pipe_products AUTO_INGEST = TRUE AS
COPY INTO stg_raw_products FROM (SELECT OBJECT_CONSTRUCT(*) FROM @s3_landing_stage)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
PATTERN = '.*products.*\.csv';

TRUNCATE TABLE indian_retail_db.bronze.stg_raw_sales;

TRUNCATE TABLE indian_retail_db.bronze.stg_raw_customers;

TRUNCATE TABLE indian_retail_db.bronze.stg_raw_products;

USE DATABASE INDIAN_RETAIL_DB;

USE SCHEMA BRONZE;

COPY INTO stg_raw_sales
FROM (
        SELECT OBJECT_CONSTRUCT (*)
        FROM @s3_landing_stage
    ) FILE_FORMAT = (
        TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1
    ) PATTERN = '.*sales.*\.csv';

COPY INTO stg_raw_customers
FROM (
        SELECT OBJECT_CONSTRUCT (*)
        FROM @s3_landing_stage
    ) FILE_FORMAT = (
        TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1
    ) PATTERN = '.*customers.*\.csv';

COPY INTO stg_raw_products
FROM (
        SELECT OBJECT_CONSTRUCT (*)
        FROM @s3_landing_stage
    ) FILE_FORMAT = (
        TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1
    ) PATTERN = '.*products.*\.csv';

SELECT COUNT(*) FROM sales_cdc_stream;

USE DATABASE INDIAN_RETAIL_DB;

USE SCHEMA BRONZE;

USE DATABASE INDIAN_RETAIL_DB;

USE SCHEMA BRONZE;

-- 1. final code Clear out any old lingering data in Bronze
TRUNCATE TABLE stg_raw_sales;

TRUNCATE TABLE stg_raw_customers;

TRUNCATE TABLE stg_raw_products;

-- 2. Manually copy historical records from S3 into their dedicated tables
COPY INTO stg_raw_sales
FROM (
        SELECT OBJECT_CONSTRUCT (*)
        FROM @s3_landing_stage
    ) FILE_FORMAT = (
        TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1
    ) PATTERN = '.*sales.*\.csv';

COPY INTO stg_raw_customers
FROM (
        SELECT OBJECT_CONSTRUCT (*)
        FROM @s3_landing_stage
    ) FILE_FORMAT = (
        TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1
    ) PATTERN = '.*customers.*\.csv';

COPY INTO stg_raw_products
FROM (
        SELECT OBJECT_CONSTRUCT (*)
        FROM @s3_landing_stage
    ) FILE_FORMAT = (
        TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1
    ) PATTERN = '.*products.*\.csv';

-- 3. Verify the rows hit your sales stream channel
SELECT COUNT(*) FROM sales_cdc_stream;

SELECT COUNT(*) FROM INDIAN_RETAIL_DB.BRONZE.SALES_CDC_STREAM;

SELECT * FROM INDIAN_RETAIL_DB.BRONZE.SALES_CDC_STREAM LIMIT 10;

USE DATABASE INDIAN_RETAIL_DB;

USE SCHEMA BRONZE;

-- 1. Update the automated continuous SALES ingestion engine
CREATE OR REPLACE PIPE indian_retail_db.bronze.pipe_sales
AUTO_INGEST = TRUE
AS
COPY INTO stg_raw_sales FROM (
  SELECT OBJECT_CONSTRUCT(
    'Order_ID', $1, 'Customer_ID', $2, 'Product_ID', $3, 'Order_Date', $4, 'Order_Time', $5,
    'Delivery_Date', $6, 'Quantity', $7, 'Unit_Price', $8, 'Order_Value', $9, 'Shipping_Cost', $10,
    'Coupon_Code', $11, 'Coupon_Discount', $12, 'Total_Amount', $13, 'Payment_Mode', $14,
    'Order_Status', $15, 'Rating', $16, 'Review_Text', $17, 'City', $18, 'State', $19,
    'Customer_Age', $20, 'Customer_Age_Group', $21
  ) FROM @s3_landing_stage
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
PATTERN = '.*sales.*\.csv';

-- 2. Update the automated continuous PRODUCTS ingestion engine
CREATE OR REPLACE PIPE indian_retail_db.bronze.pipe_products
AUTO_INGEST = TRUE
AS
COPY INTO stg_raw_products FROM (
  SELECT OBJECT_CONSTRUCT(
    'Product_ID', $1, 'Product_Name', $2, 'Category', $3, 'Brand', $4, 'Original_Price', $5,
    'Discount_Percent', $6, 'Discount_Amount', $7, 'Selling_Price', $8, 'Stock_Quantity', $9,
    'Weight_kg', $10, 'Avg_Rating', $11, 'Total_Reviews', $12
  ) FROM @s3_landing_stage
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
PATTERN = '.*products.*\.csv';

-- 3. Update the automated continuous CUSTOMERS ingestion engine
CREATE OR REPLACE PIPE indian_retail_db.bronze.pipe_customers
AUTO_INGEST = TRUE
AS
COPY INTO stg_raw_customers FROM (
  SELECT OBJECT_CONSTRUCT(
    'Customer_ID', $1, 'Customer_Name', $2, 'Gender', $3, 'Age', $4, 'Age_Group', $5,
    'Date_of_Birth', $6, 'Email', $7, 'Phone', $8, 'City', $9, 'State', $10, 'Pincode', $11,
    'Registration_Date', $12, 'Customer_Tier', $13, 'Total_Orders', $14, 'Total_Spent', $15
  ) FROM @s3_landing_stage
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
PATTERN = '.*customers.*\.csv';

DROP PIPE indian_retail_db.bronze.s3_to_bronze_pipe;

DROP STREAM indian_retail_db.bronze.orders_cdc_stream;

TRUNCATE TABLE indian_retail_db.bronze.stg_raw_sales;

TRUNCATE TABLE indian_retail_db.bronze.stg_raw_customers;

TRUNCATE TABLE indian_retail_db.bronze.stg_raw_products;

USE DATABASE INDIAN_RETAIL_DB;

-- ============================================================================
-- 1. BUILD THE GOLD LAYER TABLES (STAR SCHEMA) WITH 30-DAY TIME TRAVEL
-- ============================================================================
CREATE
OR
REPLACE
TABLE indian_retail_db.gold.fact_sales (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    order_date DATE,
    quantity INT,
    unit_price NUMBER (10, 2),
    total_amount NUMBER (12, 2),
    payment_mode STRING,
    order_status STRING
) DATA_RETENTION_TIME_IN_DAYS = 30;

CREATE
OR
REPLACE
TABLE indian_retail_db.gold.dim_customers (
    customer_id STRING,
    customer_name STRING,
    gender STRING,
    customer_tier STRING,
    city STRING,
    state STRING
) DATA_RETENTION_TIME_IN_DAYS = 30;

CREATE
OR
REPLACE
TABLE indian_retail_db.gold.dim_products (
    product_id STRING,
    product_name STRING,
    category STRING,
    brand STRING
) DATA_RETENTION_TIME_IN_DAYS = 30;

-- ============================================================================
-- 2. CREATE THE CORRECTED AUTOMATED ORCHESTRATION TASK (RUNS EVERY 30 MINUTES)
-- ============================================================================
CREATE
OR
REPLACE
    TASK indian_retail_db.gold.sync_gold_star_schema_task WAREHOUSE = COMPUTE_WH SCHEDULE = '30 MINUTE' AS
EXECUTE IMMEDIATE '
BEGIN
    -- A. Upsert records into FACT_SALES
    MERGE INTO indian_retail_db.gold.fact_sales target
    USING indian_retail_db.silver.one_big_table source
    ON target.order_id = source.order_id
    WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, product_id, order_date, quantity, unit_price, total_amount, payment_mode, order_status)
    VALUES (source.order_id, source.customer_id, source.product_id, source.order_date, source.quantity, source.unit_price, source.total_amount, source.payment_mode, source.order_status);

    -- B. Upsert records into DIM_CUSTOMERS
    MERGE INTO indian_retail_db.gold.dim_customers target
    USING (SELECT DISTINCT customer_id, customer_name, gender, customer_tier, city, state FROM indian_retail_db.silver.one_big_table) source
    ON target.customer_id = source.customer_id
    WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, gender, customer_tier, city, state)
    VALUES (source.customer_id, source.customer_name, source.gender, source.customer_tier, source.city, source.state);

    -- C. Upsert records into DIM_PRODUCTS
    MERGE INTO indian_retail_db.gold.dim_products target
    USING (SELECT DISTINCT product_id, product_name, category, brand FROM indian_retail_db.silver.one_big_table) source
    ON target.product_id = source.product_id
    WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, brand)
    VALUES (source.product_id, source.product_name, source.category, source.brand);
END;
';

-- ============================================================================
-- 3. ACTIVATE THE AUTOMATION LAYERS
-- ============================================================================
ALTER TASK indian_retail_db.gold.sync_gold_star_schema_task RESUME;

EXECUTE TASK indian_retail_db.gold.sync_gold_star_schema_task;

-- Check your Fact table count (Should show 250,000)
SELECT COUNT(*) FROM indian_retail_db.gold.fact_sales;

-- Verify your Customer dimension table has entries
SELECT * FROM indian_retail_db.gold.dim_customers LIMIT 5;

-- Verify your Product dimension table has entries
SELECT * FROM indian_retail_db.gold.dim_products LIMIT 5;

USE DATABASE INDIAN_RETAIL_DB;

-- 1. Manually load your records straight into FACT_SALES using lowercase identifiers
INSERT INTO
    indian_retail_db.gold.fact_sales (
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        unit_price,
        total_amount,
        payment_mode,
        order_status
    )
SELECT "order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "total_amount", "payment_mode", "order_status"
FROM indian_retail_db.silver.one_big_table;

-- 2. Manually populate your clean DIM_CUSTOMERS table using lowercase identifiers
INSERT INTO
    indian_retail_db.gold.dim_customers (
        customer_id,
        customer_name,
        gender,
        customer_tier,
        city,
        state
    )
SELECT DISTINCT
    "customer_id",
    "customer_name",
    "gender",
    "customer_tier",
    "city",
    "state"
FROM indian_retail_db.silver.one_big_table;

-- 3. Manually populate your clean DIM_PRODUCTS table using lowercase identifiers
INSERT INTO
    indian_retail_db.gold.dim_products (
        product_id,
        product_name,
        category,
        brand
    )
SELECT DISTINCT
    "product_id",
    "product_name",
    "category",
    "brand"
FROM indian_retail_db.silver.one_big_table;

-- This will now return exactly 250,000 live retail transactions!
SELECT COUNT(*) FROM indian_retail_db.gold.fact_sales;

USE DATABASE INDIAN_RETAIL_DB;

-- 1. Grab a clean 100-row sample of your Fact table
SELECT * FROM indian_retail_db.gold.fact_sales LIMIT 100;

-- 2. Grab a sample of your Customer dimension
SELECT * FROM indian_retail_db.gold.dim_customers LIMIT 100;

-- 3. Grab a sample of your Product dimension
SELECT * FROM indian_retail_db.gold.dim_products LIMIT 100;

EXECUTE TASK indian_retail_db.gold.sync_gold_star_schema_task;

SELECT COUNT(*) FROM indian_retail_db.gold.fact_sales;

USE DATABASE INDIAN_RETAIL_DB;

-- 1. Force load the fresh transactions into FACT_SALES
INSERT INTO
    indian_retail_db.gold.fact_sales (
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        unit_price,
        total_amount,
        payment_mode,
        order_status
    )
SELECT COALESCE(order_id, "order_id"), COALESCE(customer_id, "customer_id"), COALESCE(product_id, "product_id"), COALESCE(order_date, "order_date"), COALESCE(quantity, "quantity"), COALESCE(unit_price, "unit_price"), COALESCE(total_amount, "total_amount"), COALESCE(payment_mode, "payment_mode"), COALESCE(order_status, "order_status")
FROM indian_retail_db.silver.one_big_table
WHERE
    COALESCE(order_id, "order_id") NOT IN(
        SELECT order_id
        FROM indian_retail_db.gold.fact_sales
    );

-- 2. Force load updates into DIM_CUSTOMERS
INSERT INTO
    indian_retail_db.gold.dim_customers (
        customer_id,
        customer_name,
        gender,
        customer_tier,
        city,
        state
    )
SELECT DISTINCT
    COALESCE(customer_id, "customer_id"),
    COALESCE(
        customer_name,
        "customer_name"
    ),
    COALESCE(gender, "gender"),
    COALESCE(
        customer_tier,
        "customer_tier"
    ),
    COALESCE(city, "city"),
    COALESCE(state, "state")
FROM indian_retail_db.silver.one_big_table
WHERE
    COALESCE(customer_id, "customer_id") NOT IN(
        SELECT customer_id
        FROM indian_retail_db.gold.dim_customers
    );

-- 3. Force load updates into DIM_PRODUCTS
INSERT INTO
    indian_retail_db.gold.dim_products (
        product_id,
        product_name,
        category,
        brand
    )
SELECT DISTINCT
    COALESCE(product_id, "product_id"),
    COALESCE(product_name, "product_name"),
    COALESCE(category, "category"),
    COALESCE(brand, "brand")
FROM indian_retail_db.silver.one_big_table
WHERE
    COALESCE(product_id, "product_id") NOT IN(
        SELECT product_id
        FROM indian_retail_db.gold.dim_products
    );
