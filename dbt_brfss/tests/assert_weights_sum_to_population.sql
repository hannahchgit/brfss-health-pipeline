/*
  assert_weights_sum_to_population
  ──────────────────────────────────
  The sum of _LLCPWT within each survey year should approximate the US adult
  population (18+), which is approximately 200-280 million for 2021-2023.

  _LLCPWT is designed so that SUM(_LLCPWT) across all respondents in a year
  estimates the total civilian, non-institutionalized adult population of the US
  and participating territories.

  Returns rows only when the test FAILS (any year outside the expected range).
  This test would catch: wrong weight column used, weight column truncated/missing,
  or an accidental filter that dropped large portions of the dataset.
*/

select
    survey_year,
    round(sum(llcpwt) / 1e6, 1) as total_weight_millions,
    count(*)                     as n_respondents
from {{ ref('stg_brfss__respondents') }}
group by survey_year
having sum(llcpwt) not between 200e6 and 280e6
