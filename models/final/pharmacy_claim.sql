select
    CLAIM_ID
    ,CLAIM_LINE_NUMBER
    ,PATIENT_ID
    ,MEMBER_ID
    ,PAYER
    ,PLAN
    ,PRESCRIBING_PROVIDER_NPI
    ,DISPENSING_PROVIDER_NPI
    ,DISPENSING_DATE
    ,NDC_CODE
    ,QUANTITY
    ,DAYS_SUPPLY
    ,REFILLS
    ,PAID_DATE
    ,CHARGE_AMOUNT
    ,PAID_AMOUNT
    ,ALLOWED_AMOUNT
    ,COPAYMENT_AMOUNT
    ,COINSURANCE_AMOUNT
    ,DEDUCTIBLE_AMOUNT
    ,DATA_SOURCE
    ,in_network_flag
    ,file_name
    ,ingest_datetime
    ,member_id as person_id
from {{ ref('_int_pharmacy_claim')}}
