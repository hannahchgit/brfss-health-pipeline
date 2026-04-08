/*
  int_brfss__survey_weights
  ─────────────────────────
  Survey weight diagnostics per state and year.

  This model serves two purposes:
    1. Weight integrity checks: verify total weights approximate the US adult population
       (~250-260 million), which is the expected sum of _LLCPWT across all respondents.
    2. Supply a normalized weight column for exploratory analyses that don't need
       absolute population estimates.

  Note: For formal variance estimation (confidence intervals, design effects),
  you need statistical software that supports Taylor Series Linearization
  (e.g., R's survey package, Stata's svy commands) using _STSTR and _PSU.
  This pipeline produces point estimates only.
*/

with

staging as (
    select
        respondent_id,
        survey_year,
        state_fips,
        state_abbr,
        llcpwt,
        ststr,
        psu
    from {{ ref('stg_brfss__respondents') }}
),

year_totals as (
    select
        survey_year,
        count(*)    as n_respondents,
        sum(llcpwt) as total_weight
    from staging
    group by survey_year
),

final as (
    select
        s.respondent_id,
        s.survey_year,
        s.state_fips,
        s.state_abbr,
        s.llcpwt,
        s.ststr,
        s.psu,

        -- Normalized weight: sum to N within each year (useful for relative comparisons)
        s.llcpwt / y.total_weight * y.n_respondents   as normalized_weight,

        -- Metadata for weight QA
        y.n_respondents                                as year_n_respondents,
        y.total_weight                                 as year_total_weight,

        -- Flag respondents with extreme weights (>20x mean) for diagnostic purposes
        s.llcpwt > (y.total_weight / y.n_respondents * 20)  as is_extreme_weight

    from staging s
    join year_totals y using (survey_year)
)

select * from final
