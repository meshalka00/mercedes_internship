-- gold_cohort_retention: cohort_month x market x extra x month_n

WITH purchases AS (
  SELECT
    customer_id,
    market,
    extra_id,
    min(event_date) AS first_purchase_date
  FROM events
  WHERE event_type = 'purchase'
  GROUP BY 1,2,3
),

cohorts AS (
  SELECT
    customer_id,
    market,
    extra_id,
    date_trunc('month', first_purchase_date) AS cohort_month
  FROM purchases
),

-- monthly activity: active if at least one subscription event (purchase/renew) in that month
monthly_active AS (
  SELECT
    customer_id,
    market,
    extra_id,
    date_trunc('month', event_date) AS month
  FROM events
  WHERE event_type IN ('purchase','renew')
  GROUP BY 1,2,3,4
),

joined AS (
  SELECT
    c.cohort_month,
    c.market,
    c.extra_id,
    ma.month,
    date_diff('month', c.cohort_month, ma.month) AS month_n,
    c.customer_id
  FROM cohorts c
  JOIN monthly_active ma
    ON ma.customer_id = c.customer_id
   AND ma.market = c.market
   AND ma.extra_id = c.extra_id
  WHERE date_diff('month', c.cohort_month, ma.month) >= 0
),

cohort_sizes AS (
  SELECT cohort_month, market, extra_id, count(*) AS cohort_size
  FROM cohorts
  GROUP BY 1,2,3
)

SELECT
  j.cohort_month,
  j.market,
  j.extra_id,
  j.month_n,
  cs.cohort_size,
  count(DISTINCT j.customer_id) AS retained_subs,
  round(count(DISTINCT j.customer_id) * 1.0 / cs.cohort_size, 4) AS retention_rate
FROM joined j
JOIN cohort_sizes cs
  ON cs.cohort_month = j.cohort_month
 AND cs.market = j.market
 AND cs.extra_id = j.extra_id
GROUP BY 1,2,3,4,5;
