/*
  Exploratory: National diabetes trend 2021-2023
  ────────────────────────────────────────────────
  Run with: dbt compile && dbt show --select diabetes_prevalence_exploratory
*/

select
    survey_year,
    race_ethnicity,
    round(sum(weighted_n_diabetic) / nullif(sum(weighted_n_total), 0) * 100, 1) as diabetes_pct,
    sum(unweighted_n) as n_respondents
from {{ ref('fct_diabetes_prevalence') }}
where weighted_diabetes_prevalence is not null  -- exclude suppressed cells
group by survey_year, race_ethnicity
order by survey_year, diabetes_pct desc
