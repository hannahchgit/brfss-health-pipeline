/*
  winsorize_weights(weight_col, upper_percentile)
  ────────────────────────────────────────────────
  Caps extreme survey weights at a given percentile to reduce variance
  from highly influential observations (trim-and-rescale approach).

  This is an optional pre-processing step; CDC recommends using _LLCPWT
  as-is for official estimates. This macro is included for research workflows
  that need to investigate sensitivity to extreme weight assumptions.

  Usage:
    {{ winsorize_weights('llcpwt', 0.99) }}
*/

{% macro winsorize_weights(weight_col='llcpwt', upper_percentile=0.99) %}
    least(
        {{ weight_col }},
        percentile_cont({{ upper_percentile }}) within group (order by {{ weight_col }}) over ()
    )
{% endmacro %}
