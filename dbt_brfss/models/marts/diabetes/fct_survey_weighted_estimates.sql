/*
  fct_survey_weighted_estimates
  ──────────────────────────────
  Generic survey-weighted prevalence estimates for key BRFSS binary outcomes,
  aggregated by state × year.

  Grain: one row per (state × year).
  This is a wide mart suitable for dashboard KPI cards and state-level maps.

  All estimates use the Horvitz-Thompson estimator with _LLCPWT.
*/

with

base as (
    select
        r.respondent_id,
        r.survey_year,
        r.state_fips,
        r.state_abbr,
        r.state_name,
        r.llcpwt,
        r.bmi_category,
        r.has_hypertension,
        r.has_coronary_heart_disease,
        r.any_exercise_past_30d,
        r.physical_activity_index,
        r.urban_rural,
        d.is_diagnosed_diabetic,
        d.is_prediabetic,
        d.is_insulin_dependent,
        m.high_metabolic_risk,
        m.metabolic_risk_score
    from {{ ref('stg_brfss__respondents') }} r
    left join {{ ref('int_brfss__diabetes_flags') }}  d using (respondent_id)
    left join {{ ref('int_brfss__metabolic_risk') }}  m using (respondent_id)
),

final as (
    select
        survey_year,
        state_fips,
        state_abbr,
        state_name,

        count(*)                                                                as unweighted_n,
        sum(llcpwt)                                                             as estimated_adult_population,

        -- ── Diabetes outcomes ─────────────────────────────────────────────
        {{ survey_weighted_mean("is_diagnosed_diabetic = true") }}              as pct_diagnosed_diabetic,
        {{ survey_weighted_mean("is_prediabetic = true") }}                     as pct_prediabetic,
        {{ survey_weighted_mean("is_insulin_dependent = true") }}               as pct_insulin_dependent,

        -- ── Obesity ───────────────────────────────────────────────────────
        {{ survey_weighted_mean("bmi_category = 'Obese'") }}                    as pct_obese,
        {{ survey_weighted_mean("bmi_category in ('Overweight', 'Obese')") }}   as pct_overweight_or_obese,

        -- ── Cardiovascular ────────────────────────────────────────────────
        {{ survey_weighted_mean("has_hypertension = true") }}                   as pct_hypertensive,
        {{ survey_weighted_mean("has_coronary_heart_disease = true") }}         as pct_coronary_heart_disease,

        -- ── Physical activity ─────────────────────────────────────────────
        {{ survey_weighted_mean("any_exercise_past_30d = true") }}              as pct_any_exercise,
        {{ survey_weighted_mean("physical_activity_index = 'Inactive'") }}      as pct_inactive,

        -- ── Composite ─────────────────────────────────────────────────────
        {{ survey_weighted_mean("high_metabolic_risk = true") }}                as pct_high_metabolic_risk

    from base
    group by survey_year, state_fips, state_abbr, state_name
)

select * from final
order by survey_year, state_abbr
