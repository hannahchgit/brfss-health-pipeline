/*
  survey_weighted_mean(numerator_condition, weight_col)
  ─────────────────────────────────────────────────────
  Computes a Horvitz-Thompson weighted proportion estimate.

  This is the correct way to estimate a population prevalence from BRFSS data.
  Raw (unweighted) percentages from BRFSS are biased because the survey uses
  a complex, stratified, multi-stage probability sample where each respondent
  represents a different number of people in the target population.

  The Horvitz-Thompson estimator for a proportion p̂ is:
      p̂ = Σ(w_i * y_i) / Σ(w_i)

  where:
    w_i = survey weight (_LLCPWT) for respondent i
    y_i = 1 if respondent meets the numerator condition, 0 otherwise
    Sum is over all respondents in the domain (denominator group)

  Usage:
    {{ survey_weighted_mean("diabetes_status = 'Diagnosed'") }}
    {{ survey_weighted_mean("bmi_category = 'Obese'", weight_col='llcpwt') }}

  Returns a value between 0 and 1 (proportion), or NULL if sum of weights is 0.
*/

{% macro survey_weighted_mean(numerator_condition, weight_col='llcpwt') %}
    sum(case when {{ numerator_condition }} then {{ weight_col }} else 0 end)
    / nullif(sum({{ weight_col }}), 0)
{% endmacro %}


/*
  weighted_count(condition, weight_col)
  ─────────────────────────────────────
  Returns the population-weighted count (estimated number of people in the US
  adult population satisfying the condition).
*/

{% macro weighted_count(condition, weight_col='llcpwt') %}
    sum(case when {{ condition }} then {{ weight_col }} else 0 end)
{% endmacro %}
