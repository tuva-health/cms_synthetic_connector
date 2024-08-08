with inpatient_base_claim as (
select 
  *
, left(clm_thru_dt,4) as clm_thru_dt_year
from {{ source('cms_synthetic','inpatient') }}
),

/* Claim ID is not unique across claim types.  Concatenating original claim ID, claim year, and claim type. */
add_claim_id as (
select
  *
, cast(clm_id as {{ dbt.type_string() }} ) || cast(clm_thru_dt_year as {{ dbt.type_string() }} ) || cast(nch_clm_type_cd as {{ dbt.type_string() }} ) as claim_id
from inpatient_base_claim

),

header_payment as (
select
  claim_id
, sum(cast(clm_pmt_amt as {{ dbt.type_numeric() }})) as paid_amount
, sum(cast(clm_tot_chrg_amt as {{ dbt.type_numeric() }})) as charge_amount
from add_claim_id
group by 1
) 

select
      b.claim_id
    , cast(clm_line_num as integer) as claim_line_number
    , 'institutional' as claim_type
    , cast(b.bene_id as {{ dbt.type_string() }} ) as patient_id
    , cast(b.bene_id as {{ dbt.type_string() }} ) as member_id
    , cast('medicare_synthetic' as {{ dbt.type_string() }} ) as payer
    , cast('medicare_synthetic' as {{ dbt.type_string() }} ) as plan
    , {{ try_to_cast_date('b.clm_admsn_dt', 'DD-MON-YYYY') }} as claim_start_date
    , {{ try_to_cast_date('b.clm_thru_dt', 'DD-MON-YYYY') }} as claim_end_date
    , {{ try_to_cast_date('b.clm_thru_dt', 'DD-MON-YYYY') }} as claim_line_start_date
    , {{ try_to_cast_date('b.clm_thru_dt', 'DD-MON-YYYY') }} as claim_line_end_date
    , {{ try_to_cast_date('b.clm_admsn_dt', 'DD-MON-YYYY') }} as admission_date
    , {{ try_to_cast_date('b.nch_bene_dschrg_dt', 'DD-MON-YYYY') }} as discharge_date
    , cast(b.clm_src_ip_admsn_cd as {{ dbt.type_string() }} ) as admit_source_code
    , cast(b.clm_ip_admsn_type_cd as {{ dbt.type_string() }} ) as admit_type_code
    , cast(b.ptnt_dschrg_stus_cd as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(NULL as {{ dbt.type_string() }} ) as place_of_service_code
    , cast(b.clm_fac_type_cd as {{ dbt.type_string() }} )
        || cast(b.clm_srvc_clsfctn_type_cd as {{ dbt.type_string() }} )
        || cast(b.clm_freq_cd as {{ dbt.type_string() }} )
      as bill_type_code
    , cast(b.clm_drg_cd as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(NULL as {{ dbt.type_string() }} ) as apr_drg_code
    , cast(rev_cntr as {{ dbt.type_string() }} ) as revenue_center_code
    , cast(null as integer) as service_unit_quantity
    , cast(hcpcs_cd as {{ dbt.type_string() }} ) as hcpcs_code
    , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_1
    , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_2
    , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_3
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_4
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_5
    , cast(b.at_physn_npi as {{ dbt.type_string() }} ) as rendering_npi
    , cast(b.org_npi_num as {{ dbt.type_string() }} ) as billing_npi
    , cast(b.org_npi_num as {{ dbt.type_string() }} ) as facility_npi
    , date(NULL) as paid_date
    , coalesce(p.paid_amount,cast(0 as {{ dbt.type_numeric() }})) as paid_amount
    , cast(NULL as {{ dbt.type_numeric() }}) as allowed_amount
    , p.charge_amount as charge_amount
    , cast(null as {{ dbt.type_numeric() }}) as coinsurance_amount
    , cast(null as {{ dbt.type_numeric() }}) as copayment_amount
    , cast(null as {{ dbt.type_numeric() }}) as deductible_amount
    , cast(NULL as {{ dbt.type_numeric() }}) as total_cost_amount
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
    , cast(b.clm_poa_ind_sw1 as {{ dbt.type_string() }} ) as diagnosis_poa_1
    , cast(b.clm_poa_ind_sw2 as {{ dbt.type_string() }} ) as diagnosis_poa_2
    , cast(b.clm_poa_ind_sw3 as {{ dbt.type_string() }} ) as diagnosis_poa_3
    , cast(b.clm_poa_ind_sw4 as {{ dbt.type_string() }} ) as diagnosis_poa_4
    , cast(b.clm_poa_ind_sw5 as {{ dbt.type_string() }} ) as diagnosis_poa_5
    , cast(b.clm_poa_ind_sw6 as {{ dbt.type_string() }} ) as diagnosis_poa_6
    , cast(b.clm_poa_ind_sw7 as {{ dbt.type_string() }} ) as diagnosis_poa_7
    , cast(b.clm_poa_ind_sw8 as {{ dbt.type_string() }} ) as diagnosis_poa_8
    , cast(b.clm_poa_ind_sw9 as {{ dbt.type_string() }} ) as diagnosis_poa_9
    , cast(b.clm_poa_ind_sw10 as {{ dbt.type_string() }} ) as diagnosis_poa_10
    , cast(b.clm_poa_ind_sw11 as {{ dbt.type_string() }} ) as diagnosis_poa_11
    , cast(b.clm_poa_ind_sw12 as {{ dbt.type_string() }} ) as diagnosis_poa_12
    , cast(b.clm_poa_ind_sw13 as {{ dbt.type_string() }} ) as diagnosis_poa_13
    , cast(b.clm_poa_ind_sw14 as {{ dbt.type_string() }} ) as diagnosis_poa_14
    , cast(b.clm_poa_ind_sw15 as {{ dbt.type_string() }} ) as diagnosis_poa_15
    , cast(b.clm_poa_ind_sw16 as {{ dbt.type_string() }} ) as diagnosis_poa_16
    , cast(b.clm_poa_ind_sw17 as {{ dbt.type_string() }} ) as diagnosis_poa_17
    , cast(b.clm_poa_ind_sw18 as {{ dbt.type_string() }} ) as diagnosis_poa_18
    , cast(b.clm_poa_ind_sw19 as {{ dbt.type_string() }} ) as diagnosis_poa_19
    , cast(b.clm_poa_ind_sw20 as {{ dbt.type_string() }} ) as diagnosis_poa_20
    , cast(b.clm_poa_ind_sw21 as {{ dbt.type_string() }} ) as diagnosis_poa_21
    , cast(b.clm_poa_ind_sw22 as {{ dbt.type_string() }} ) as diagnosis_poa_22
    , cast(b.clm_poa_ind_sw23 as {{ dbt.type_string() }} ) as diagnosis_poa_23
    , cast(b.clm_poa_ind_sw24 as {{ dbt.type_string() }} ) as diagnosis_poa_24
    , cast(b.clm_poa_ind_sw25 as {{ dbt.type_string() }} ) as diagnosis_poa_25
    , 'icd-10-pcs' as procedure_code_type
    , cast(b.icd_prcdr_cd1 as {{ dbt.type_string() }} ) as procedure_code_1
    , cast(b.icd_prcdr_cd2 as {{ dbt.type_string() }} ) as procedure_code_2
    , cast(b.icd_prcdr_cd3 as {{ dbt.type_string() }} ) as procedure_code_3
    , cast(b.icd_prcdr_cd4 as {{ dbt.type_string() }} ) as procedure_code_4
    , cast(b.icd_prcdr_cd5 as {{ dbt.type_string() }} ) as procedure_code_5
    , cast(b.icd_prcdr_cd6 as {{ dbt.type_string() }} ) as procedure_code_6
    , cast(b.icd_prcdr_cd7 as {{ dbt.type_string() }} ) as procedure_code_7
    , cast(b.icd_prcdr_cd8 as {{ dbt.type_string() }} ) as procedure_code_8
    , cast(b.icd_prcdr_cd9 as {{ dbt.type_string() }} ) as procedure_code_9
    , cast(b.icd_prcdr_cd10 as {{ dbt.type_string() }} ) as procedure_code_10
    , cast(b.icd_prcdr_cd11 as {{ dbt.type_string() }} ) as procedure_code_11
    , cast(b.icd_prcdr_cd12 as {{ dbt.type_string() }} ) as procedure_code_12
    , cast(b.icd_prcdr_cd13 as {{ dbt.type_string() }} ) as procedure_code_13
    , cast(b.icd_prcdr_cd14 as {{ dbt.type_string() }} ) as procedure_code_14
    , cast(b.icd_prcdr_cd15 as {{ dbt.type_string() }} ) as procedure_code_15
    , cast(b.icd_prcdr_cd16 as {{ dbt.type_string() }} ) as procedure_code_16
    , cast(b.icd_prcdr_cd17 as {{ dbt.type_string() }} ) as procedure_code_17
    , cast(b.icd_prcdr_cd18 as {{ dbt.type_string() }} ) as procedure_code_18
    , cast(b.icd_prcdr_cd19 as {{ dbt.type_string() }} ) as procedure_code_19
    , cast(b.icd_prcdr_cd20 as {{ dbt.type_string() }} ) as procedure_code_20
    , cast(b.icd_prcdr_cd21 as {{ dbt.type_string() }} ) as procedure_code_21
    , cast(b.icd_prcdr_cd22 as {{ dbt.type_string() }} ) as procedure_code_22
    , cast(b.icd_prcdr_cd23 as {{ dbt.type_string() }} ) as procedure_code_23
    , cast(b.icd_prcdr_cd24 as {{ dbt.type_string() }} ) as procedure_code_24
    , cast(b.icd_prcdr_cd25 as {{ dbt.type_string() }} ) as procedure_code_25
    , {{ try_to_cast_date('b.prcdr_dt1', 'DD-MON-YYYY') }} as procedure_date_1
    , {{ try_to_cast_date('b.prcdr_dt2', 'DD-MON-YYYY') }} as procedure_date_2
    , {{ try_to_cast_date('b.prcdr_dt3', 'DD-MON-YYYY') }} as procedure_date_3
    , {{ try_to_cast_date('b.prcdr_dt4', 'DD-MON-YYYY') }} as procedure_date_4
    , {{ try_to_cast_date('b.prcdr_dt5', 'DD-MON-YYYY') }} as procedure_date_5
    , {{ try_to_cast_date('b.prcdr_dt6', 'DD-MON-YYYY') }} as procedure_date_6
    , {{ try_to_cast_date('b.prcdr_dt7', 'DD-MON-YYYY') }} as procedure_date_7
    , {{ try_to_cast_date('b.prcdr_dt8', 'DD-MON-YYYY') }} as procedure_date_8
    , {{ try_to_cast_date('b.prcdr_dt9', 'DD-MON-YYYY') }} as procedure_date_9
    , {{ try_to_cast_date('b.prcdr_dt10', 'DD-MON-YYYY') }} as procedure_date_10
    , {{ try_to_cast_date('b.prcdr_dt11', 'DD-MON-YYYY') }} as procedure_date_11
    , {{ try_to_cast_date('b.prcdr_dt12', 'DD-MON-YYYY') }} as procedure_date_12
    , {{ try_to_cast_date('b.prcdr_dt13', 'DD-MON-YYYY') }} as procedure_date_13
    , {{ try_to_cast_date('b.prcdr_dt14', 'DD-MON-YYYY') }} as procedure_date_14
    , {{ try_to_cast_date('b.prcdr_dt15', 'DD-MON-YYYY') }} as procedure_date_15
    , {{ try_to_cast_date('b.prcdr_dt16', 'DD-MON-YYYY') }} as procedure_date_16
    , {{ try_to_cast_date('b.prcdr_dt17', 'DD-MON-YYYY') }} as procedure_date_17
    , {{ try_to_cast_date('b.prcdr_dt18', 'DD-MON-YYYY') }} as procedure_date_18
    , {{ try_to_cast_date('b.prcdr_dt19', 'DD-MON-YYYY') }} as procedure_date_19
    , {{ try_to_cast_date('b.prcdr_dt20', 'DD-MON-YYYY') }} as procedure_date_20
    , {{ try_to_cast_date('b.prcdr_dt21', 'DD-MON-YYYY') }} as procedure_date_21
    , {{ try_to_cast_date('b.prcdr_dt22', 'DD-MON-YYYY') }} as procedure_date_22
    , {{ try_to_cast_date('b.prcdr_dt23', 'DD-MON-YYYY') }} as procedure_date_23
    , {{ try_to_cast_date('b.prcdr_dt24', 'DD-MON-YYYY') }} as procedure_date_24
    , {{ try_to_cast_date('b.prcdr_dt25', 'DD-MON-YYYY') }} as procedure_date_25
    , 'medicare_synthetic' as data_source
    , 1 as in_network_flag
    , 'inpatient_claim' as file_name
    , cast(NULL as date ) as ingest_datetime
from add_claim_id as b
inner join header_payment p on b.claim_id = p.claim_id
