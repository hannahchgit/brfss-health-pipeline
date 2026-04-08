/*
  int_brfss__diabetes_flags
  ─────────────────────────
  Boolean diabetes classification flags for each respondent.
  Centralizes diabetes logic so marts don't repeat CASE WHEN conditions.
*/

with

staging as (
    select
        respondent_id,
        survey_year,
        llcpwt,
        diabetes_status,
        has_prediabetes,
        takes_insulin
    from {{ ref('stg_brfss__respondents') }}
),

final as (
    select
        respondent_id,
        survey_year,
        llcpwt,

        diabetes_status = 'Diagnosed'                       as is_diagnosed_diabetic,
        diabetes_status = 'Prediabetes' or has_prediabetes  as is_prediabetic,
        takes_insulin = true                                as is_insulin_dependent,

        case
            when diabetes_status = 'Diagnosed' and takes_insulin = true
                then 'Insulin-Dependent Diabetic'
            when diabetes_status = 'Diagnosed'
                then 'Diagnosed Diabetic'
            when diabetes_status = 'Prediabetes' or has_prediabetes
                then 'Prediabetic'
            when diabetes_status = 'No'
                then 'No Diabetes'
            else 'Unknown'
        end                                                 as diabetes_category

    from staging
)

select * from final
