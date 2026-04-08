/*
  dim_respondent_demographics
  ────────────────────────────
  Demographic attributes for each BRFSS respondent.
  Grain: one row per respondent_id (unique).

  This dimension table separates demographic attributes from outcomes/measures,
  enabling clean star-schema joins from fact tables.
*/

select
    respondent_id,
    survey_year,
    state_fips,
    state_abbr,
    state_name,
    interview_month,
    interview_year,
    sex,
    age_group,
    race_ethnicity,
    race_ethnicity_collapsed,
    education_level,
    income_group,
    urban_rural
from {{ ref('stg_brfss__respondents') }}
