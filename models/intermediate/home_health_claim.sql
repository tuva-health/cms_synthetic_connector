with hha_base_claim as (

    select *
         , right(clm_thru_dt,4) as clm_thru_dt_year
    from {{ ref('hha') }}

    /** filter out denied claims **/
)
, header_payment as (
    select
        clm_id as claim_id
        ,clm_line_num
        , cast(clm_pmt_amt as {{ dbt.type_numeric() }}) as paid_amount
        , /** medicare payment **/
          cast(clm_pmt_amt as {{ dbt.type_numeric() }}) 
          /** primary payer payment **/
          + cast(nch_prmry_pyr_clm_pd_amt as {{ dbt.type_numeric() }})
        as total_cost_amount
        , cast(clm_tot_chrg_amt as {{ dbt.type_numeric() }}) as charge_amount
    from hha_base_claim
    where clm_line_num =1
)

, claim_start_date as (

  select l.clm_id
  ,min(coalesce(rev_cntr_dt,l.clm_thru_dt)) as claim_start_date
  from {{ ref('hha') }} l
  group by l.clm_id
)

select
      /* Claim ID is not unique across claim types.  Concatenating original claim ID, claim year, and claim type. */
      cast(b.clm_id as {{ dbt.type_string() }} )
        || cast(b.clm_thru_dt_year as {{ dbt.type_string() }} )
        || cast(b.nch_clm_type_cd as {{ dbt.type_string() }} )
      as claim_id
    , cast(b.clm_line_num as integer) as claim_line_number
    , 'institutional' as claim_type
    , cast(b.bene_id as {{ dbt.type_string() }} ) as patient_id
    , cast(b.bene_id as {{ dbt.type_string() }} ) as member_id
    , cast('medicare' as {{ dbt.type_string() }} ) as payer
    , cast('medicare' as {{ dbt.type_string() }} ) as plan
    , {{ try_to_cast_date('s.claim_start_date', 'DD-MON-YYYY') }} as claim_start_date
    , {{ try_to_cast_date('b.clm_thru_dt', 'DD-MON-YYYY') }} as claim_end_date
    , {{ try_to_cast_date('rev_cntr_dt', 'DD-MON-YYYY') }} as claim_line_start_date
    , {{ try_to_cast_date('rev_cntr_dt', 'DD-MON-YYYY') }} as claim_line_end_date
    , {{ try_to_cast_date('b.clm_admsn_dt', 'DD-MON-YYYY') }} as admission_date
    , {{ try_to_cast_date('b.clm_thru_dt', 'DD-MON-YYYY') }} as discharge_date
    , cast(NULL as {{ dbt.type_string() }} ) as admit_source_code
    , cast(NULL as {{ dbt.type_string() }} ) as admit_type_code
    , cast(right('00' || b.ptnt_dschrg_stus_cd,2)  as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(NULL as {{ dbt.type_string() }} ) as place_of_service_code
    , cast(b.clm_fac_type_cd as {{ dbt.type_string() }} )
        || cast(b.clm_srvc_clsfctn_type_cd as {{ dbt.type_string() }} )
        || cast(b.clm_freq_cd as {{ dbt.type_string() }} )
      as bill_type_code
    , cast(NULL as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(NULL as {{ dbt.type_string() }} ) as apr_drg_code
    , cast(rev_cntr as {{ dbt.type_string() }} ) as revenue_center_code
    , cast(regexp_substr(rev_cntr_unit_cnt, '.') as integer) as service_unit_quantity
    , cast(hcpcs_cd as {{ dbt.type_string() }} ) as hcpcs_code
    , cast(hcpcs_1st_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_1
    , cast(hcpcs_2nd_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_2
    , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_3
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_4
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_5
    , cast(RNDRNG_PHYSN_NPI as {{ dbt.type_string() }} ) as rendering_npi
    , cast(b.org_npi_num as {{ dbt.type_string() }} ) as billing_npi
    , cast(b.org_npi_num as {{ dbt.type_string() }} ) as facility_npi
    , date(NULL) as paid_date
    , p.paid_amount as paid_amount
    , cast(NULL as {{ dbt.type_numeric() }}) as allowed_amount
    , p.charge_amount as charge_amount
    , cast(null as {{ dbt.type_numeric() }}) as coinsurance_amount
    , cast(null as {{ dbt.type_numeric() }}) as copayment_amount
    , cast(null as {{ dbt.type_numeric() }}) as deductible_amount
    , p.total_cost_amount as total_cost_amount
    , 'icd-10-cm' as diagnosis_code_type
    , cast(b.prncpal_dgns_cd as {{ dbt.type_string() }} ) as diagnosis_code_1
    , cast(b.icd_dgns_cd2 as {{ dbt.type_string() }} ) as diagnosis_code_2
    , cast(b.icd_dgns_cd3 as {{ dbt.type_string() }} ) as diagnosis_code_3
    , cast(b.icd_dgns_cd4 as {{ dbt.type_string() }} ) as diagnosis_code_4
    , cast(b.icd_dgns_cd5 as {{ dbt.type_string() }} ) as diagnosis_code_5
    , cast(b.icd_dgns_cd6 as {{ dbt.type_string() }} ) as diagnosis_code_6
    , cast(b.icd_dgns_cd7 as {{ dbt.type_string() }} ) as diagnosis_code_7
    , cast(b.icd_dgns_cd8 as {{ dbt.type_string() }} ) as diagnosis_code_8
    , cast(b.icd_dgns_cd9 as {{ dbt.type_string() }} ) as diagnosis_code_9
    , cast(b.icd_dgns_cd10 as {{ dbt.type_string() }} ) as diagnosis_code_10
    , cast(b.icd_dgns_cd11 as {{ dbt.type_string() }} ) as diagnosis_code_11
    , cast(b.icd_dgns_cd12 as {{ dbt.type_string() }} ) as diagnosis_code_12
    , cast(b.icd_dgns_cd13 as {{ dbt.type_string() }} ) as diagnosis_code_13
    , cast(b.icd_dgns_cd14 as {{ dbt.type_string() }} ) as diagnosis_code_14
    , cast(b.icd_dgns_cd15 as {{ dbt.type_string() }} ) as diagnosis_code_15
    , cast(b.icd_dgns_cd16 as {{ dbt.type_string() }} ) as diagnosis_code_16
    , cast(b.icd_dgns_cd17 as {{ dbt.type_string() }} ) as diagnosis_code_17
    , cast(b.icd_dgns_cd18 as {{ dbt.type_string() }} ) as diagnosis_code_18
    , cast(b.icd_dgns_cd19 as {{ dbt.type_string() }} ) as diagnosis_code_19
    , cast(b.icd_dgns_cd20 as {{ dbt.type_string() }} ) as diagnosis_code_20
    , cast(b.icd_dgns_cd21 as {{ dbt.type_string() }} ) as diagnosis_code_21
    , cast(b.icd_dgns_cd22 as {{ dbt.type_string() }} ) as diagnosis_code_22
    , cast(b.icd_dgns_cd23 as {{ dbt.type_string() }} ) as diagnosis_code_23
    , cast(b.icd_dgns_cd24 as {{ dbt.type_string() }} ) as diagnosis_code_24
    , cast(b.icd_dgns_cd25 as {{ dbt.type_string() }} ) as diagnosis_code_25
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_1
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_2
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_3
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_4
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_5
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_6
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_7
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_8
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_9
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_10
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_11
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_12
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_13
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_14
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_15
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_16
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_17
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_18
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_19
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_20
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_21
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_22
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_23
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_24
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_25
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_type
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_1
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_2
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_3
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_4
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_5
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_6
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_7
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_8
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_9
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_10
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_11
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_12
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_13
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_14
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_15
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_16
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_17
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_18
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_19
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_20
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_21
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_22
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_23
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_24
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_25
    , date(NULL) as procedure_date_1
    , date(NULL) as procedure_date_2
    , date(NULL) as procedure_date_3
    , date(NULL) as procedure_date_4
    , date(NULL) as procedure_date_5
    , date(NULL) as procedure_date_6
    , date(NULL) as procedure_date_7
    , date(NULL) as procedure_date_8
    , date(NULL) as procedure_date_9
    , date(NULL) as procedure_date_10
    , date(NULL) as procedure_date_11
    , date(NULL) as procedure_date_12
    , date(NULL) as procedure_date_13
    , date(NULL) as procedure_date_14
    , date(NULL) as procedure_date_15
    , date(NULL) as procedure_date_16
    , date(NULL) as procedure_date_17
    , date(NULL) as procedure_date_18
    , date(NULL) as procedure_date_19
    , date(NULL) as procedure_date_20
    , date(NULL) as procedure_date_21
    , date(NULL) as procedure_date_22
    , date(NULL) as procedure_date_23
    , date(NULL) as procedure_date_24
    , date(NULL) as procedure_date_25
    , 'cms_synthetic' as data_source
    , 1 as in_network_flag
    , 'home_health_claim' as file_name
    , cast(NULL as date ) as ingest_datetime
from hha_base_claim as b
left join claim_start_date s on b.clm_id = s.clm_id
left join header_payment p
    on b.clm_id = p.claim_id
and
p.clm_line_num = b.clm_line_num