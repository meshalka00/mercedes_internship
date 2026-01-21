# Data Dictionary — Digital Extras Analytics Demo

This project uses a **synthetic but realistic** dataset inspired by connected-car digital subscriptions ("digital extras").
Data is generated for demo purposes and contains no real customer information.

## Entities (High-level)
- **Markets**: countries/regions where digital extras are offered (multi-market analytics).
- **Customers**: users grouped by market and segment.
- **Digital extras**: subscription-based features/services.
- **Events**: user lifecycle events (trial, purchase, renew, cancel) and usage activity.

---

## Tables

### 1) dim_market
**Grain:** one row per market  
**Primary key:** `market`

| Column | Type | Example | Description |
|---|---|---|---|
| market | string | DE | Market / country code |
| region | string | EU | Region grouping (EU, NA, APAC, etc.) |

---

### 2) dim_extra
**Grain:** one row per digital extra  
**Primary key:** `extra_id`

| Column | Type | Example | Description |
|---|---|---|---|
| extra_id | string | EX_03 | Unique extra identifier |
| extra_name | string | Navigation+ | Human-readable name |
| category | string | Comfort | Product category |
| price_monthly | numeric | 9.99 | Monthly price (for MRR) |

---

### 3) dim_customer
**Grain:** one row per customer  
**Primary key:** `customer_id`

| Column | Type | Example | Description |
|---|---|---|---|
| customer_id | string | C_000123 | Unique customer identifier |
| market | string | DE | FK → dim_market.market |
| segment | string | Private | Customer segment (e.g., Private / Business / Premium) |
| signup_date | date | 2025-03-12 | First seen / signup date (synthetic) |

---

### 4) fact_events
**Grain:** one row per event  
**Primary key:** (none; dedup rules apply)

| Column | Type | Example | Description |
|---|---|---|---|
| event_ts | timestamp | 2025-05-01 10:15:00 | Event timestamp |
| event_date | date | 2025-05-01 | Partition date derived from event_ts |
| customer_id | string | C_000123 | FK → dim_customer.customer_id |
| market | string | DE | Redundant for convenience; should match dim_customer.market |
| extra_id | string | EX_03 | FK → dim_extra.extra_id |
| event_type | string | purchase | See event types below |
| quantity | integer | 1 | Optional (e.g., sessions count for usage events) |

**Event types (controlled vocabulary)**
- `trial_start` — customer starts a trial for an extra
- `purchase` — customer purchases/subscribes to an extra
- `renew` — subscription renewal
- `cancel` — cancellation / churn event
- `usage_session` — usage activity event (can be repeated)

**Business rules (expected order)**
- `trial_start` may happen before `purchase` (optional)
- `purchase` should happen before `renew`
- `cancel` can happen after `purchase`
- `usage_session` can happen after `trial_start` or `purchase` (depends on extra)

---

## Derived / Gold Tables (for BI)

### gold_daily_kpi
**Grain:** one row per day × market × extra

| Column | Description |
|---|---|
| date | KPI date |
| market, region | Market dimensions |
| extra_id, category | Extra dimensions |
| trials | Count of trial_start events |
| purchases | Count of purchase events |
| renewals | Count of renew events |
| cancels | Count of cancel events |
| active_subscriptions | Estimated active subs (see methodology) |
| mrr | Monthly recurring revenue estimate |
| active_users | Unique customers with usage_session that day |
| sessions | Total usage_session events that day |

### gold_cohort_retention
**Grain:** cohort_month × market × extra × month_n

| Column | Description |
|---|---|
| cohort_month | month of first purchase |
| month_n | 0,1,2,3... months since purchase |
| cohort_size | number of purchasers in cohort |
| retained_subs | active subs in month_n |
| retention_rate | retained_subs / cohort_size |

### gold_dq_results
**Grain:** date × check_name

| Column | Description |
|---|---|
| date | run date |
| check_name | e.g., missing_keys, duplicates, invalid_sequence |
| table_name | table checked |
| severity | info / warn / fail |
| failed_rows | count of failed rows |
| sample_keys | small sample of offending keys (optional) |

---

## KPI Definitions (Minimum Set)

### Funnel
- **Trial → Purchase conversion** = purchases / trials (by market/extra)
- **Time to convert** = median days between trial_start and purchase

### Revenue
- **MRR** = active_subscriptions × price_monthly
- **ARPU** = MRR / active_subscriptions (for sanity; equals price if simple)

### Retention / Churn
- **Churn rate (period)** = cancels / active_subscriptions
- **Cohort retention** = retained_subs / cohort_size

### Usage
- **DAU for extra** = active_users
- **Usage per subscriber** = sessions / active_subscriptions
- **Activation gap** = share of purchasers with 0 usage in first 7 days

---

## Notes
- Dataset is synthetic. Patterns (conversion, churn, usage) are intentionally varied by market and extra to enable meaningful insights.
