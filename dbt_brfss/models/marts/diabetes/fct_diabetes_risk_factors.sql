/*
  fct_diabetes_risk_factors
  ──────────────────────────
  Weighted prevalence of metabolic risk factors stratified by diabetes status and year.
  Grain: one row per (risk_factor × diabetes_category × survey_year).

  This mart enables direct comparison of risk factor burden between:
    - Diagnosed Diabetics
    - Prediabetics
    - Non-Diabetics

  All prevalence estimates use the Horvitz-Thompson estimator.
*/

with

base as (
    select
        r.respondent_id,
        r.survey_year,
        r.llcpwt,
        d.diabetes_category,
        m.obese_flag,
        m.hypertension_flag,
        m.inactive_flag,
        m.no_exercise_flag,
        m.poor_diet_flag,
        m.metabolic_risk_score,
        m.high_metabolic_risk
    from {{ ref('stg_brfss__respondents') }} r
    left join {{ ref('int_brfss__diabetes_flags') }}  d using (respondent_id)
    left join {{ ref('int_brfss__metabolic_risk') }}  m using (respondent_id)
    where d.diabetes_category in ('Diagnosed Diabetic', 'Insulin-Dependent Diabetic', 'Prediabetic', 'No Diabetes')
),

final as (
    select
        survey_year,
        diabetes_category,
        count(*)                                                    as unweighted_n,
        sum(llcpwt)                                                 as weighted_n,

        -- Prevalence of each risk factor within this diabetes category
        {{ survey_weighted_mean("obese_flag = 1") }}                as pct_obese,
        {{ survey_weighted_mean("hypertension_flag = 1") }}         as pct_hypertensive,
        {{ survey_weighted_mean("inactive_flag = 1") }}             as pct_inactive,
        {{ survey_weighted_mean("no_exercise_flag = 1") }}          as pct_no_exercise,
        {{ survey_weighted_mean("poor_diet_flag = 1") }}            as pct_poor_diet,
        {{ survey_weighted_mean("high_metabolic_risk = true") }}    as pct_high_metabolic_risk,

        -- Mean metabolic risk score
        sum(metabolic_risk_score * llcpwt)
            / nullif(sum(case when metabolic_risk_score is not null then llcpwt end), 0)
                                                                    as mean_metabolic_risk_score

    from base
    group by survey_year, diabetes_category
)

select * from final
order by survey_year, diabetes_category
