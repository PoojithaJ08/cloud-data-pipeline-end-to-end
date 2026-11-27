-- stg_customers — dbt staging model (demonstration sample)
-- One row per customer: trims strings, normalises code fields to upper
-- case, and dedupes on the natural key (customer_id), keeping the most
-- recently loaded copy — the same cleansing the Spark pipeline applies
-- (see ../../pyspark_etl_sample.py). Source is the raw landed extract.

with source as (

    select * from {{ source('sap_extracts', 'customers_raw') }}

),

deduped as (

    select
        trim(customer_id)   as customer_id,
        trim(name)          as name,
        trim(sales_org)     as sales_org,
        upper(trim(country)) as country,
        trim(email)         as email,
        credit_limit,
        row_number() over (
            partition by trim(customer_id)
            order by loaded_at desc
        ) as rn
    from source

)

select
    customer_id,
    name,
    sales_org,
    country,
    email,
    credit_limit
from deduped
where rn = 1
