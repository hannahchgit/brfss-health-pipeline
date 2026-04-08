/*
  fct_diabetes_prevalence
  ────────────────────────
  Survey-weighted diabetes prevalence by state × year × age_group × race_ethnicity.

  Grain: one row per unique combination of the four dimensions above.
  Only strata with ≥ 50 unweighted respondents are included (suppression rule).

  Key columns:
    - weighted_diabetes_prevalence: Horvitz-Thompson proportion estimate (0-1)
    - weighted_n_diabetic:          Estimated number of diabetic adults in the stratum
    - weighted_n_total:             Estimated total adults in the stratum (sum of weights)
    - unweighted_n:                 Actual survey respondent count (for data confidence)
*/

with

respondents as (
    select
        r.respondent_id,
        r.survey_year,
        r.state_fips,
        r.state_abbr,
        r.state_name,
        r.age_group,
        r.race_ethnicity_collapsed  as race_ethnicity,
        r.llcpwt,
        d.is_diagnosed_diabetic,
        d.is_prediabetic,
        d.is_insulin_dependent,
        d.diabetes_category
    from {{ ref('stg_brfss__respondents') }} r
    left join {{ ref('int_brfss__diabetes_flags') }} d using (respondent_id)
    where r.age_group is not null
      and r.race_ethnicity_collapsed is not null
),

aggregated as (
    select
        survey_year,
        state_fips,
        state_abbr,
        state_name,
        age_group,
        race_ethnicity,

        -- ── Survey-weighted estimates (Horvitz-Thompson) ──────────────────
        {{ survey_weighted_mean("is_diagnosed_diabetic = true") }}
            as weighted_diabetes_prevalence,

        {{ survey_weighted_mean("is_prediabetic = true") }}
            as weighted_prediabetes_prevalence,

        {{ survey_weighted_mean("is_insulin_dependent = true") }}
            as weighted_insulin_dependent_prevalence,

        -- ── Population counts ─────────────────────────────────────────────
        {{ weighted_count("is_diagnosed_diabetic = true") }}
            as weighted_n_diabetic,

        sum(llcpwt)
            as weighted_n_total,

        -- ── Unweighted respondent counts (for data quality / suppression) ─
        count(*)                                as unweighted_n,
        count(case when is_diagnosed_diabetic = true  then 1 end) as unweighted_n_diabetic

    from respondents
    group by
        survey_year, state_fips, state_abbr, state_name, age_group, race_ethnicity
),

-- Apply small-cell suppression: hide strata with fewer than 50 respondents
final as (
    select
        survey_year,
        state_fips,
        state_abbr,
        state_name,
        age_group,
        race_ethnicity,
        unweighted_n,
        unweighted_n_diabetic,
        weighted_n_total,
        weighted_n_diabetic,
        case when unweighted_n >= 50 then weighted_diabetes_prevalence     end as weighted_diabetes_prevalence,
        case when unweighted_n >= 50 then weighted_prediabetes_prevalence  end as weighted_prediabetes_prevalence,
        case when unweighted_n >= 50 then weighted_insulin_dependent_prevalence end as weighted_insulin_dependent_prevalence
    from aggregated
)

select * from final
order by survey_year, state_abbr, age_group, race_ethnicity
