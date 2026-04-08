# BRFSS Variables Reference

## What is BRFSS?

The Behavioral Risk Factor Surveillance System (BRFSS) is the world's largest ongoing
telephone health survey, conducted annually by the CDC in collaboration with all 50 US
states, the District of Columbia, and participating US territories.

- **Target population**: Civilian, non-institutionalized adults aged 18+ living in
  private residences or college housing
- **Sample size**: ~400,000–500,000 respondents per year
- **Design**: Stratified, multi-stage random-digit-dialing probability sample

---

## Why Raw Percentages are Wrong

A naive analyst might compute:

```sql
-- WRONG: ignores complex survey design
SELECT AVG(CASE WHEN diabete4 = 1 THEN 1.0 ELSE 0 END) as pct_diabetic
FROM raw_brfss;
```

This is biased for two reasons:

1. **Unequal selection probabilities**: Urban residents, cellphone-only households,
   and certain demographic groups are sampled at different rates. Without weighting,
   oversampled groups distort national estimates.

2. **Post-stratification adjustment**: CDC adjusts weights to match known Census
   population distributions (age, sex, race/ethnicity, phone ownership).
   Ignoring this means your estimates don't represent the US population.

### The Correct Approach: Horvitz-Thompson Estimation

```sql
-- CORRECT: Horvitz-Thompson weighted proportion
SELECT
    SUM(CASE WHEN diabete4 = 1 THEN _llcpwt ELSE 0 END)
    / NULLIF(SUM(_llcpwt), 0) as pct_diabetic
FROM raw_brfss;
```

This is implemented as the `survey_weighted_mean()` macro in this project.

---

## Survey Weight Variables

### `_LLCPWT` — Final Combined Weight

The primary weight to use for **all** population-level estimates.

- Each respondent's weight represents the number of US adults they "represent"
  in the target population
- `SUM(_LLCPWT)` across all respondents in a year ≈ US adult population (~250–260 million)
- Used in all mart models and the `survey_weighted_mean` macro

```sql
-- Verify weight sums approximate US adult population
SELECT survey_year, SUM(llcpwt) / 1e6 as estimated_adults_millions
FROM staging.stg_brfss__respondents
GROUP BY survey_year;
-- Expected: ~250-260 million
```

### `_STSTR` — Stratum

Used with `_PSU` for **variance estimation** via Taylor Series Linearization (TSL).

TSL is the gold standard for computing standard errors and confidence intervals
from complex survey data. This project produces point estimates only; for confidence
intervals, use R's `survey` package or Stata's `svy` commands with these two variables.

### `_PSU` — Primary Sampling Unit

The first-stage sampling unit (geographic area). Used with `_STSTR` for TSL variance
estimation. Do not confuse with individual respondents.

---

## Key Diabetes Variables

### `DIABETE4` — Diabetes Diagnosis

**Question**: "Has a doctor, nurse, or other health professional ever told you that
you have diabetes?"

| Code | Label | Staging Decode |
|------|-------|---------------|
| 1 | Yes | `'Diagnosed'` |
| 2 | Yes, during pregnancy only | `'Gestational Only'` |
| 3 | No | `'No'` |
| 4 | No, prediabetes/borderline diabetes | `'Prediabetes'` |
| 7 | Don't know / Not sure | `'Unknown'` |
| 9 | Refused | `'Unknown'` |

**Note**: Code changed from `DIABETE3` to `DIABETE4` in 2016 (added code 4 for prediabetes).

### `PREDIAB2` — Prediabetes

**Question**: "Have you ever been told by a doctor or other health professional
that you have prediabetes or borderline diabetes?"

| Code | Label |
|------|-------|
| 1 | Yes |
| 2 | No |
| 7 | Don't know |
| 9 | Refused |

### `INSULIN1` — Insulin Use

**Question**: "Are you now taking insulin?"

---

## BMI Variables

### `_BMI5` — Calculated BMI (Raw)

**Important**: CDC stores BMI as an integer multiplied by 100.

- `_BMI5 = 2750` → BMI = 27.50
- The staging model divides by 100.0: `bmi5 / 100.0 AS bmi_calc`
- Missing: `9999` → mapped to `NULL`

### `_BMI5CAT` — BMI Category

| Code | Category | BMI Range |
|------|----------|-----------|
| 1 | Underweight | < 18.5 |
| 2 | Normal weight | 18.5–24.9 |
| 3 | Overweight | 25.0–29.9 |
| 4 | Obese | ≥ 30.0 |
| 9 | Missing | — |

---

## Demographic Variables

### `SEXVAR` — Sex (2021+)

Variable renamed from `SEX1` (2018-2020) and `SEX` (pre-2018). This pipeline
uses 2021–2023 data, so `SEXVAR` is always available.

| Code | Label |
|------|-------|
| 1 | Male |
| 2 | Female |

### `_AGEG5YR` — Age in 5-Year Groups

| Code | Age Group |
|------|-----------|
| 1 | 18–24 |
| 2 | 25–29 |
| ... | ... |
| 13 | 80+ |
| 14 | Missing |

### `_RACE1` — Race/Ethnicity (7 Categories)

Uses a hierarchy prioritizing Hispanic ethnicity over race.
Missing (code 9) includes respondents who refused or said "Don't know."

---

## Variable Stability Across Years

BRFSS variable names are generally stable but do change. Key changes in 2021–2023:

| Variable | Status |
|---------|--------|
| `SEXVAR` | Renamed from `SEX1` in 2021; stable 2021–2023 |
| `DIABETE4` | Stable since 2016 |
| `_BMI5` / `_BMI5CAT` | Stable |
| `INCOME3` | Expanded categories added in 2021 (now 1-11, was 1-8) |
| `_RACE1` | Stable |

Always check the BRFSS codebook for the relevant year at:
[https://www.cdc.gov/brfss/annual_data/annual_data.htm](https://www.cdc.gov/brfss/annual_data/annual_data.htm)

---

## Known Limitations

1. **Self-reported data**: All BRFSS variables are self-reported, subject to social
   desirability bias and recall error. Diabetes diagnosis, in particular, excludes
   undiagnosed cases (estimated ~20% of all diabetics in the US).

2. **Telephone survey**: Excludes people without phones and those in institutions
   (nursing homes, prisons). Response rates have declined (~50% in early years to
   ~45% in 2021-2023).

3. **Point estimates only**: This pipeline does not compute confidence intervals.
   For inference, use R `survey::svyglm()` or Stata `svy:` with `_STSTR` and `_PSU`.

4. **Variance estimation**: TSL confidence intervals require software that understands
   the nested STSTR/PSU structure. DuckDB SQL alone cannot do this correctly.
