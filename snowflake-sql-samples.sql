-- ============================================================================
-- Snowflake Load Patterns — Staging -> Dedupe -> Merge
-- Poojitha Krishna Jonnalagadda — Data Engineer (Python, SQL, Spark/PySpark)
--
-- DEMONSTRATION SAMPLE: The warehouse-side companion to the pipeline in this
-- project. The PySpark/local pipeline (pyspark_etl_sample.py,
-- run_pipeline_local.py) cleanses file extracts; when the target is
-- Snowflake, the same three moves happen in SQL: land raw rows in a staging
-- table, dedupe on the natural key, then MERGE into the governed target.
-- Table and column names are illustrative; no client data or systems.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- STEP 1: STAGING — land the extract exactly as received.
-- A staging table is disposable: truncate and reload every run, keep the
-- source columns plus load metadata so any bad row can be traced back.
-- (In production the COPY INTO lands files from the S3/Azure stage; here the
--  staging table is assumed loaded from sample_input/customers.csv.)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_customers_raw (
    customer_id   VARCHAR,
    name          VARCHAR,
    sales_org     VARCHAR,
    country       VARCHAR,
    email         VARCHAR,
    credit_limit  NUMBER(18, 2),
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file  VARCHAR
);

-- COPY INTO stg_customers_raw
-- FROM @sap_extract_stage/customers.csv
-- FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 NULL_IF = ('', 'NULL'));


-- ----------------------------------------------------------------------------
-- STEP 2: DEDUPE + CLEANSE into a conformed staging table.
-- QUALIFY ROW_NUMBER() keeps one row per natural key (the most recently
-- loaded copy); code fields are normalised to upper case, strings trimmed —
-- the same cleansing the Spark pipeline applies.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE stg_customers AS
SELECT  customer_id,
        name,
        sales_org,
        country,
        email,
        credit_limit
FROM (
    SELECT  TRIM(customer_id)                          AS customer_id,
            TRIM(name)                                 AS name,
            TRIM(sales_org)                            AS sales_org,
            UPPER(TRIM(country))                       AS country,
            TRIM(email)                                AS email,
            credit_limit,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(customer_id)
                ORDER BY _loaded_at DESC)              AS rn
    FROM    stg_customers_raw
)
WHERE rn = 1;


-- ----------------------------------------------------------------------------
-- STEP 3: VALIDATE before merge — the checks that gate the load.
-- Each should return 0 before the MERGE runs; failures go back to the data
-- owners in an error log, the same discipline as the file-based pipeline.
-- ----------------------------------------------------------------------------
-- 3a. Mandatory fields present
SELECT COUNT(*) AS missing_mandatory
FROM   stg_customers
WHERE  customer_id IS NULL OR name IS NULL OR sales_org IS NULL;

-- 3b. Email present and well-formed
SELECT COUNT(*) AS bad_email
FROM   stg_customers
WHERE  email IS NULL
   OR  NOT email REGEXP '^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$';

-- 3c. Credit limit sane
SELECT COUNT(*) AS bad_credit_limit
FROM   stg_customers
WHERE  credit_limit IS NULL OR credit_limit < 0;


-- ----------------------------------------------------------------------------
-- STEP 4: MERGE into the governed target (gold).
-- Matched rows are updated in place, new rows inserted — an idempotent
-- upsert, so re-running a load after a fix never duplicates the target.
-- ----------------------------------------------------------------------------
MERGE INTO dim_customer t
USING stg_customers s
   ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET
    t.name         = s.name,
    t.sales_org    = s.sales_org,
    t.country      = s.country,
    t.email        = s.email,
    t.credit_limit = s.credit_limit,
    t.updated_at   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (customer_id, name, sales_org, country, email, credit_limit, updated_at)
VALUES
    (s.customer_id, s.name, s.sales_org, s.country, s.email, s.credit_limit,
     CURRENT_TIMESTAMP());


-- ----------------------------------------------------------------------------
-- STEP 5: RECONCILE — the sign-off count.
-- Target row count must equal the distinct cleansed source keys; a variance
-- means the merge, not the business, has a problem.
-- ----------------------------------------------------------------------------
SELECT (SELECT COUNT(*) FROM stg_customers)   AS staged_keys,
       (SELECT COUNT(*) FROM dim_customer)    AS target_rows,
       (SELECT COUNT(*) FROM stg_customers)
         - (SELECT COUNT(*) FROM dim_customer) AS variance;  -- must be 0
