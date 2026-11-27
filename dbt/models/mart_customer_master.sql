-- mart_customer_master — dbt mart model (demonstration sample)
-- The governed customer master for reporting: only records that pass the
-- validation rules (mandatory fields present, email well-formed, credit
-- limit sane). Records that fail are NOT silently dropped — the tests in
-- schema.yml fail the build, and the rejects are reviewed with the data
-- owners, the same error-log discipline as the file-based pipeline
-- (see ../../run_pipeline_local.py).

with staged as (

    select * from {{ ref('stg_customers') }}

),

validated as (

    select *
    from staged
    where customer_id is not null
      and name is not null
      and sales_org is not null
      and email is not null
      and email like '%@%.%'
      and credit_limit >= 0

)

select
    customer_id,
    name,
    sales_org,
    country,
    email,
    credit_limit
from validated
