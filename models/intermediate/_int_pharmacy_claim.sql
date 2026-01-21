with pde as (
  SELECT * FROM
  {% if False %} {{ ref('pde') }} {% else %} {{ source('cms_synthetic', 'pde') }} {% endif %}
)

select
    cast(pde_id as {{ dbt.type_string() }}) as CLAIM_ID
    ,cast(1 as {{ dbt.type_string() }}) as CLAIM_LINE_NUMBER
    ,cast(bene_id as {{ dbt.type_string() }}) as PATIENT_ID
    ,cast(bene_id as {{ dbt.type_string() }}) as MEMBER_ID
    ,cast('medicare' as {{ dbt.type_string() }}) as PAYER
    ,cast('medicare' as {{ dbt.type_string() }}) as PLAN
    ,cast(prscrbr_id as {{ dbt.type_string() }}) as PRESCRIBING_PROVIDER_NPI
    ,cast(null as {{ dbt.type_string() }}) as DISPENSING_PROVIDER_NPI
    ,{{ try_to_cast_date('srvc_dt', 'DD-MON-YYYY') }} as DISPENSING_DATE
    ,cast(prod_srvc_id as {{ dbt.type_string() }}) as NDC_CODE
    ,cast(qty_dspnsd_num as int) as QUANTITY
    ,cast(days_suply_num as int) as DAYS_SUPPLY
    ,cast(fill_num as int) as REFILLS
    ,{{ try_to_cast_date('pd_dt', 'DD-MON-YYYY') }} as PAID_DATE
    ,cast(null as numeric) as CHARGE_AMOUNT
    ,cast(cvrd_d_plan_pd_amt as numeric) as PAID_AMOUNT
    ,cast(tot_rx_cst_amt as numeric) as ALLOWED_AMOUNT
    ,cast(plro_amt as numeric) as COPAYMENT_AMOUNT
    ,cast(null as numeric) as COINSURANCE_AMOUNT
    ,cast(null as numeric) as DEDUCTIBLE_AMOUNT
    ,cast('cms_synthetic' as {{ dbt.type_string() }}) as DATA_SOURCE
    ,cast(1 as numeric) as in_network_flag
    ,cast('pde' as {{ dbt.type_string() }}) as file_name
    ,cast(NULL as date ) as file_date
    ,cast(NULL as date ) as ingest_datetime
from pde