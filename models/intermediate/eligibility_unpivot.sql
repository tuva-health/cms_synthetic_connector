with demographics as (

    select * from {{ ref('beneficiary') }}

),

year_from_file as (
select *
,REGEXP_SUBSTR(source_file, '\\d{4}') AS reference_year
from demographics
)

,unpivot_dual_status as (

    select
          bene_id
        , right(month,2) as month
        , reference_year as year
        , dual_status as dual_status
    from year_from_file
    unpivot(
            dual_status for month in (DUAL_STUS_CD_01
                                      ,DUAL_STUS_CD_02
                                      ,DUAL_STUS_CD_03
                                      ,DUAL_STUS_CD_04
                                      ,DUAL_STUS_CD_05
                                      ,DUAL_STUS_CD_06
                                      ,DUAL_STUS_CD_07
                                      ,DUAL_STUS_CD_08
                                      ,DUAL_STUS_CD_09
                                      ,DUAL_STUS_CD_10
                                      ,DUAL_STUS_CD_11
                                      ,DUAL_STUS_CD_12)
            )p1

),

unpivot_medicare_status as (

    select
          bene_id
        , right(month,2) as month
        , reference_year as year
        , medicare_status
    from year_from_file
    unpivot(
            medicare_status for month in (MDCR_STATUS_CODE_01
                                          ,MDCR_STATUS_CODE_02
                                          ,MDCR_STATUS_CODE_03
                                          ,MDCR_STATUS_CODE_04
                                          ,MDCR_STATUS_CODE_05
                                          ,MDCR_STATUS_CODE_06
                                          ,MDCR_STATUS_CODE_07
                                          ,MDCR_STATUS_CODE_08
                                          ,MDCR_STATUS_CODE_09
                                          ,MDCR_STATUS_CODE_10
                                          ,MDCR_STATUS_CODE_11
                                          ,MDCR_STATUS_CODE_12)
            )p1

)

,

unpivot_hmo_status as (
    select
          bene_id
        , case when LENGTH(month) = 14 then substring(month, 14, 1)  -- 'MDCR_ENTLMT_BUYIN_IND_1' -> '1'
               when LENGTH(month) = 15 then substring(month, 14, 2)  -- 'MDCR_ENTLMT_BUYIN_IND_10' -> '10'
               else null end as month
        , reference_year as year
        , hmo_status
    from year_from_file
    unpivot(
            hmo_status for month in (hmo_ind_01
                                          ,hmo_ind_02
                                          ,hmo_ind_03
                                          ,hmo_ind_04
                                          ,hmo_ind_05
                                          ,hmo_ind_06
                                          ,hmo_ind_07
                                          ,hmo_ind_08
                                          ,hmo_ind_09
                                          ,hmo_ind_10
                                          ,hmo_ind_11
                                          ,hmo_ind_12
                                          )
            )p1


)

,

unpivot_entitlement as (

    select
          bene_id
        , case when LENGTH(month) = 23 then substring(month, 23, 1)  -- 'MDCR_ENTLMT_BUYIN_IND_1' -> '1'
               when LENGTH(month) = 24 then substring(month, 23, 2)  -- 'MDCR_ENTLMT_BUYIN_IND_10' -> '10'
               else null end as month
        , reference_year as year
        , entitlement
    from year_from_file
    unpivot(
            entitlement for month in (MDCR_ENTLMT_BUYIN_IND_01
                                          ,MDCR_ENTLMT_BUYIN_IND_02
                                          ,MDCR_ENTLMT_BUYIN_IND_03
                                          ,MDCR_ENTLMT_BUYIN_IND_04
                                          ,MDCR_ENTLMT_BUYIN_IND_05
                                          ,MDCR_ENTLMT_BUYIN_IND_06
                                          ,MDCR_ENTLMT_BUYIN_IND_07
                                          ,MDCR_ENTLMT_BUYIN_IND_08
                                          ,MDCR_ENTLMT_BUYIN_IND_09
                                          ,MDCR_ENTLMT_BUYIN_IND_10
                                          ,MDCR_ENTLMT_BUYIN_IND_11
                                          ,MDCR_ENTLMT_BUYIN_IND_12
                                          )
            )p1

)


select
      year_from_file.bene_id as desy_sort_key
    , year_from_file.age_at_end_ref_yr as age
    , year_from_file.sex_ident_cd as sex_code
    , year_from_file.bene_race_cd as race_code
    , year_from_file.state_code as state_code
    , year_from_file.bene_death_dt as date_of_death
    , year_from_file.BENE_HI_CVRAGE_TOT_MONS as hi_coverage
    , year_from_file.BENE_SMI_CVRAGE_TOT_MONS as smi_coverage
    , year_from_file.BENE_HMO_CVRAGE_TOT_MONS as hmo_coverage
    , year_from_file.ENTLMT_RSN_ORIG as orig_reason_for_entitlement
    , unpivot_dual_status.dual_status as dual_status
    , unpivot_medicare_status.medicare_status as medicare_status
    , unpivot_dual_status.month as month
    , unpivot_dual_status.year as year
    , concat(
          unpivot_dual_status.year
        , unpivot_dual_status.month
      ) as year_month
    , unpivot_hmo_status.hmo_status
    , unpivot_entitlement.entitlement
from year_from_file
     inner join unpivot_dual_status
         on year_from_file.bene_id = unpivot_dual_status.bene_id
         and year_from_file.reference_year = unpivot_dual_status.year
     left join unpivot_medicare_status
         on unpivot_dual_status.bene_id = unpivot_medicare_status.bene_id
         and unpivot_dual_status.month = unpivot_medicare_status.month
         and unpivot_dual_status.year = unpivot_medicare_status.year
     left join unpivot_hmo_status
         on unpivot_dual_status.bene_id = unpivot_hmo_status.bene_id
         and cast(unpivot_dual_status.month as int) = cast(unpivot_hmo_status.month as int)
         and unpivot_dual_status.year = unpivot_hmo_status.year
     left join unpivot_entitlement
         on unpivot_dual_status.bene_id = unpivot_entitlement.bene_id
         and cast(unpivot_dual_status.month as int) = cast(unpivot_entitlement.month as int)
         and unpivot_dual_status.year = unpivot_entitlement.year