with beneficiary as (
    select * from {% if var('demo_data_only', false) %} {{ ref('beneficiary') }} {% else %} {{ source('cms_synthetic', 'beneficiary') }} {% endif %}
),

beneficiary_with_year_from_filename as (
    select *
        , {{ regexp_substr("source_file", "'\\d{4}'") }}  AS reference_year
    from beneficiary
),

pivoted_to_columns as (
    -- state_cnty_fips_cd
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'state_cnty_fips_cd' as data_type
        , state_cnty_fips_cd as val
    from beneficiary_with_year_from_filename
    unpivot(
        state_cnty_fips_cd for month in (STATE_CNTY_FIPS_CD_01, STATE_CNTY_FIPS_CD_02, STATE_CNTY_FIPS_CD_03, STATE_CNTY_FIPS_CD_04
                                , STATE_CNTY_FIPS_CD_05, STATE_CNTY_FIPS_CD_06, STATE_CNTY_FIPS_CD_07, STATE_CNTY_FIPS_CD_08
                                , STATE_CNTY_FIPS_CD_09, STATE_CNTY_FIPS_CD_10, STATE_CNTY_FIPS_CD_11, STATE_CNTY_FIPS_CD_12)
    ) p1

    union all
    -- mdcr_status_code
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'mdcr_status_code' as data_type
        , mdcr_status_code as val
    from beneficiary_with_year_from_filename
    unpivot(
        mdcr_status_code for month in (mdcr_status_code_01, mdcr_status_code_02, mdcr_status_code_03, mdcr_status_code_04
                                ,mdcr_status_code_05, mdcr_status_code_06, mdcr_status_code_07, mdcr_status_code_08
                                ,mdcr_status_code_09, mdcr_status_code_10, mdcr_status_code_11, mdcr_status_code_12)
    ) p1

    union all
    -- mdcr_entlmnt_buyin_ind
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'mdcr_entlmnt_buyin_ind' as data_type
        , mdcr_entlmnt_buyin_ind as val
    from beneficiary_with_year_from_filename
    unpivot(
        mdcr_entlmnt_buyin_ind for month in (MDCR_ENTLMT_BUYIN_IND_01, MDCR_ENTLMT_BUYIN_IND_02, MDCR_ENTLMT_BUYIN_IND_03
                                , MDCR_ENTLMT_BUYIN_IND_04, MDCR_ENTLMT_BUYIN_IND_05, MDCR_ENTLMT_BUYIN_IND_06
                                , MDCR_ENTLMT_BUYIN_IND_07, MDCR_ENTLMT_BUYIN_IND_08, MDCR_ENTLMT_BUYIN_IND_09
                                , MDCR_ENTLMT_BUYIN_IND_10, MDCR_ENTLMT_BUYIN_IND_11, MDCR_ENTLMT_BUYIN_IND_12)
    ) p1

    union all
    -- hmo_ind
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'hmo_ind' as data_type
        , hmo_ind as val
    from beneficiary_with_year_from_filename
    unpivot(
        hmo_ind for month in (hmo_ind_01, hmo_ind_02, hmo_ind_03, hmo_ind_04, hmo_ind_05
                               , hmo_ind_06, hmo_ind_07, hmo_ind_08, hmo_ind_09, hmo_ind_10
                               , hmo_ind_11, hmo_ind_12)
    ) p1

     union all 
    -- ptc_cntrct_id
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'ptc_cntrct_id' as data_type
        , ptc_cntrct_id as val
    from beneficiary_with_year_from_filename
    unpivot(
        ptc_cntrct_id for month in (ptc_cntrct_id_01, ptc_cntrct_id_02, ptc_cntrct_id_03, ptc_cntrct_id_04, ptc_cntrct_id_05
                               , ptc_cntrct_id_06, ptc_cntrct_id_07, ptc_cntrct_id_08, ptc_cntrct_id_09, ptc_cntrct_id_10
                               , ptc_cntrct_id_11, ptc_cntrct_id_12)
    ) p1

    union all
    -- ptc_pbp_id
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'ptc_pbp_id' as data_type
        , ptc_pbp_id as val
    from beneficiary_with_year_from_filename
    unpivot(
        ptc_pbp_id for month in (ptc_pbp_id_01, ptc_pbp_id_02, ptc_pbp_id_03, ptc_pbp_id_04, ptc_pbp_id_05
                               , ptc_pbp_id_06, ptc_pbp_id_07, ptc_pbp_id_08, ptc_pbp_id_09, ptc_pbp_id_10
                               , ptc_pbp_id_11, ptc_pbp_id_12)
    ) p1

    union all
    -- ptc_plan_type_cd
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'ptc_plan_type_cd' as data_type
        , ptc_plan_type_cd as val
    from beneficiary_with_year_from_filename
    unpivot(
        ptc_plan_type_cd for month in (ptc_plan_type_cd_01, ptc_plan_type_cd_02, ptc_plan_type_cd_03, ptc_plan_type_cd_04, ptc_plan_type_cd_05
                               , ptc_plan_type_cd_06, ptc_plan_type_cd_07, ptc_plan_type_cd_08, ptc_plan_type_cd_09, ptc_plan_type_cd_10
                               , ptc_plan_type_cd_11, ptc_plan_type_cd_12)
    ) p1

     union all
    -- ptd_cntrct_id
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'ptd_cntrct_id' as data_type
        , ptd_cntrct_id as val
    from beneficiary_with_year_from_filename
    unpivot(
        ptd_cntrct_id for month in (ptd_cntrct_id_01, ptd_cntrct_id_02, ptd_cntrct_id_03, ptd_cntrct_id_04, ptd_cntrct_id_05
                               , ptd_cntrct_id_06, ptd_cntrct_id_07, ptd_cntrct_id_08, ptd_cntrct_id_09, ptd_cntrct_id_10
                               , ptd_cntrct_id_11, ptd_cntrct_id_12)
    ) p1


    union all
    -- ptd_pbp_id
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'ptd_pbp_id' as data_type
        , ptd_pbp_id as val
    from beneficiary_with_year_from_filename
    unpivot(
        ptd_pbp_id for month in (ptd_pbp_id_01, ptd_pbp_id_02, ptd_pbp_id_03, ptd_pbp_id_04, ptd_pbp_id_05
                               , ptd_pbp_id_06, ptd_pbp_id_07, ptd_pbp_id_08, ptd_pbp_id_09, ptd_pbp_id_10
                               , ptd_pbp_id_11, ptd_pbp_id_12)
    ) p1

    union all
    -- ptd_sgmnt_id
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'ptd_sgmt_id' as data_type
        , ptd_sgmt_id as val
    from beneficiary_with_year_from_filename
    unpivot(
        ptd_sgmt_id for month in (ptd_sgmt_id_01, ptd_sgmt_id_02, ptd_sgmt_id_03, ptd_sgmt_id_04, ptd_sgmt_id_05
                               , ptd_sgmt_id_06, ptd_sgmt_id_07, ptd_sgmt_id_08, ptd_sgmt_id_09, ptd_sgmt_id_10
                               , ptd_sgmt_id_11, ptd_sgmt_id_12)
    ) p1

    union all
    -- rds_ind
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'rds_ind' as data_type
        , rds_ind as val
    from beneficiary_with_year_from_filename
    unpivot(
        rds_ind for month in (rds_ind_01, rds_ind_02, rds_ind_03, rds_ind_04, rds_ind_05
                               , rds_ind_06, rds_ind_07, rds_ind_08, rds_ind_09, rds_ind_10
                               , rds_ind_11, rds_ind_12)
    ) p1

    union all
    -- dual_stus_cd
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'dual_stus_cd' as data_type
        , dual_stus_cd as val
    from beneficiary_with_year_from_filename
    unpivot(
        dual_stus_cd for month in (DUAL_STUS_CD_01, DUAL_STUS_CD_02, DUAL_STUS_CD_03, DUAL_STUS_CD_04
                                , DUAL_STUS_CD_05, DUAL_STUS_CD_06, DUAL_STUS_CD_07, DUAL_STUS_CD_08
                                , DUAL_STUS_CD_09, DUAL_STUS_CD_10, DUAL_STUS_CD_11, DUAL_STUS_CD_12)
    ) p1

    union all 
    -- cst_shr_grp_cd
    select
        bene_id
        , right(month, 2) as month
        , reference_year as year
        , 'cst_shr_grp_cd' as data_type
        , cst_shr_grp_cd as val
    from beneficiary_with_year_from_filename
    unpivot(
        cst_shr_grp_cd for month in (cst_shr_grp_cd_01, cst_shr_grp_cd_02, cst_shr_grp_cd_03, cst_shr_grp_cd_04
                                , cst_shr_grp_cd_05, cst_shr_grp_cd_06, cst_shr_grp_cd_07, cst_shr_grp_cd_08
                                , cst_shr_grp_cd_09, cst_shr_grp_cd_10, cst_shr_grp_cd_11, cst_shr_grp_cd_12)
    ) p1
),
    

summary_by_bene_month as (
    select
        bene_id
        , month
        , year
        , max(case when data_type = 'state_cnty_fips_cd' then val end) as state_cnty_fips_cd
        , max(case when data_type = 'mdcr_status_code' then val end) as mdcr_status_code
        , max(case when data_type = 'mdcr_entlmnt_buyin_ind' then val end) as mdcr_entlmnt_buyin_ind
        , max(case when data_type = 'hmo_ind' then val end) as hmo_ind
        , max(case when data_type = 'ptc_cntrct_id' then val end) as ptc_cntrct_id
        , max(case when data_type = 'ptc_pbp_id' then val end) as ptc_pbp_id
        , max(case when data_type = 'ptc_plan_type_cd' then val end) as ptc_plan_type_cd
        , max(case when data_type = 'ptd_cntrct_id' then val end) as ptd_cntrct_id
        , max(case when data_type = 'ptd_pbp_id' then val end) as ptd_pbp_id
        , max(case when data_type = 'ptd_sgmt_id' then val end) as ptd_sgmt_id
        , max(case when data_type = 'rds_ind' then val end) as rds_ind
        , max(case when data_type = 'dual_stus_cd' then val end) as dual_stus_cd
        , max(case when data_type = 'cst_shr_grp_cd' then val end) as cst_shr_grp_cd
    from pivoted_to_columns
    where month is not null
    group by bene_id, month, year
)

select
    beneficiary_with_year_from_filename.bene_id as bene_id
    , summary_by_bene_month.month as month
    , summary_by_bene_month.year as year
    , concat(summary_by_bene_month.year, summary_by_bene_month.month) as year_month
    , beneficiary_with_year_from_filename.age_at_end_ref_yr as age
    , beneficiary_with_year_from_filename.sex_ident_cd as sex_code
    , beneficiary_with_year_from_filename.bene_race_cd as race_code
    , beneficiary_with_year_from_filename.state_code as state_code
    , beneficiary_with_year_from_filename.bene_death_dt as date_of_death
    , beneficiary_with_year_from_filename.BENE_HI_CVRAGE_TOT_MONS as hi_coverage
    , beneficiary_with_year_from_filename.BENE_SMI_CVRAGE_TOT_MONS as smi_coverage
    , beneficiary_with_year_from_filename.BENE_HMO_CVRAGE_TOT_MONS as hmo_coverage
    , beneficiary_with_year_from_filename.ENTLMT_RSN_ORIG as orig_reason_for_entitlement
    , summary_by_bene_month.state_cnty_fips_cd
    , summary_by_bene_month.mdcr_status_code
    , summary_by_bene_month.mdcr_entlmnt_buyin_ind 
    , summary_by_bene_month.hmo_ind 
    , summary_by_bene_month.ptc_cntrct_id     
    , summary_by_bene_month.ptc_pbp_id 
    , summary_by_bene_month.ptc_plan_type_cd 
    , summary_by_bene_month.ptd_cntrct_id 
    , summary_by_bene_month.ptd_pbp_id 
    , summary_by_bene_month.ptd_sgmt_id 
    , summary_by_bene_month.rds_ind 
    , summary_by_bene_month.dual_stus_cd
    , summary_by_bene_month.cst_shr_grp_cd
    from beneficiary_with_year_from_filename
    inner join summary_by_bene_month
        on beneficiary_with_year_from_filename.bene_id = summary_by_bene_month.bene_id
        and beneficiary_with_year_from_filename.reference_year = summary_by_bene_month.year