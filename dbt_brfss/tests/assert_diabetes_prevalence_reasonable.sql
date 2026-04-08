/*
  assert_diabetes_prevalence_reasonable
  ────────────────────────────────────────
  National weighted diabetes prevalence should be between 8% and 15% for 2021-2023.

  CDC's 2021-2023 published national diabetes prevalence is approximately 11-12%.
  This test catches catastrophic errors in weight application or diabetes variable decoding.

  Returns rows only when the test FAILS (any year outside the 8-15% range).
*/

with national_prevalence as (
    select
        survey_year,
        sum(case when weighted_diabetes_prevalence is not null then weighted_n_diabetic else 0 end)
            / nullif(sum(case when weighted_diabetes_prevalence is not null then weighted_n_total else null end), 0)
            as national_weighted_prevalence
    from {{ ref('fct_diabetes_prevalence') }}
    group by survey_year
)

select
    survey_year,
    round(national_weighted_prevalence * 100, 2) as prevalence_pct
from national_prevalence
where national_weighted_prevalence not between 0.08 and 0.15
