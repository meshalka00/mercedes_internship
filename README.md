# Mercedes Internship Demo — Digital Extras Business Analytics

End-to-end demo project for **Digital Product Business Analytics**: synthetic event data → gold KPI tables → BI dashboard + data quality monitoring.

> Note: Power BI Desktop is Windows-only. The dashboard is built in **Looker Studio** on macOS and exported as screenshots.

## Tech stack
- Python (data generation + transformation)
- SQL (readable KPI + cohort queries)
- Pandas (gold table builds)
- DuckDB (optional) (if you use the DuckDB pipeline variant)
- Git/GitHub (version control)
- Looker Studio (BI dashboard on macOS)
- Conceptual mapping to Azure/Databricks/Power BI: the pipeline mirrors a typical cloud analytics workflow (raw → curated/gold → BI)

## What this project demonstrates
- Business analytics for digital extras (trial → purchase → renew/cancel → usage)
- Stakeholder-ready reporting across markets (overview + funnel + retention)
- Data quality monitoring for event logs (duplicates, invalid sequences, missing keys)
- Clear, reproducible workflow and documentation

## Dashboard (Looker Studio)

### Overview
![Overview](dashboard/screenshots/overview.png)

### Funnel
![Funnel](dashboard/screenshots/funnel.png)

### Cohorts
![Cohorts](dashboard/screenshots/cohorts.png)

### Data Quality
![Data Quality](dashboard/screenshots/data_quality.png)

## Key insights (from the demo run)
- Overall trial → purchase conversion ~18%
- US contributes the largest share of MRR in this synthetic dataset; GB shows the highest conversion among top markets (~26%)
- Data Quality monitoring flags duplicate events (~91) and renew-before-purchase anomalies (~29) (noise injected for demo)

## Data model (synthetic)

Dimensions
- dim_market — market → region
- dim_extra — extra_id → category, price_monthly
- dim_customer — customer_id → market, segment

Fact
- fact_events — event log with: trial_start, purchase, renew, cancel, usage_session

## Gold tables (outputs)

Generated into data/processed/:

- gold_daily_kpi — daily metrics per date × market × extra_id
  - trials, purchases, renewals, cancels, active_users, sessions
  - active_subscriptions (proxy) = cumulative(purchases + renewals − cancels)
  - mrr (proxy) = active_subscriptions × price_monthly

- gold_cohort_retention — cohort retention per cohort_month × market × extra_id × month_n
  - cohort_month = month of first purchase
  - retained_subs = customers with purchase/renew in month_n
  - retention_rate = retained_subs / cohort_size

- gold_dq_results — data quality checks
  - missing keys, duplicates, invalid sequences (renew before purchase), etc.

> Subscription and MRR calculations are simplified proxies for demo purposes. In production, these typically rely on contract/billing tables and precise subscription lifecycle logic.

## How to reproduce locally

### 1) Requirements
- Python 3.10+ recommended
- macOS / Linux / Windows supported

Install packages (minimal):
```bash
python3 -m pip install -U pandas pyarrow

## 2) Generate raw synthetic data

Run the generator (example parameters):
```bash
python3 src/generate_data.py \
  --out data/raw \
  --n_customers 25000 \
  --seed 42 \
  --format csv \
  --quality_noise 0.02
  
## 3) Build gold tables (KPI + cohorts + DQ)
python3 src/build_gold_tiny.py


