-- gold_daily_kpi_simple
-- 1 row per date x market x extra_id
-- Metrics are event-based counts + a simple proxy for active subscriptions.

WITH daily_events AS (
  SELECT
    event_date AS date,
    market,
    extra_id,
    sum(CASE WHEN event_type = 'trial_start' THEN 1 ELSE 0 END)  AS trials,
    sum(CASE WHEN event_type = 'purchase'    THEN 1 ELSE 0 END)  AS purchases,
    sum(CASE WHEN event_type = 'renew'       THEN 1 ELSE 0 END)  AS renewals,
    sum(CASE WHEN event_type = 'cancel'      THEN 1 ELSE 0 END)  AS cancels,
    count(DISTINCT CASE WHEN event_type = 'usage_session' THEN customer_id END) AS active_users,
    sum(CASE WHEN event_type = 'usage_session' THEN 1 ELSE 0 END) AS sessions
  FROM events
  GROUP BY 1,2,3
)

SELECT
  de.date,
  de.market,
  m.region,
  de.extra_id,
  x.category,
  de.trials,
  de.purchases,
  de.renewals,
  de.cancels,
  -- Simple proxy: cumulative net adds = purchases + renewals - cancels
  sum(de.purchases + de.renewals - de.cancels)
    OVER (PARTITION BY de.market, de.extra_id ORDER BY de.date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS active_subscriptions,
  de.active_users,
  de.sessions,
  round(
    (sum(de.purchases + de.renewals - de.cancels)
      OVER (PARTITION BY de.market, de.extra_id ORDER BY de.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) * x.price_monthly, 2
  ) AS mrr
FROM daily_events de
JOIN markets m ON m.market = de.market
JOIN extras  x ON x.extra_id = de.extra_id;
