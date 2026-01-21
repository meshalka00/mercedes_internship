#!/usr/bin/env python3
from __future__ import annotations

import os
import glob
import pandas as pd


RAW_DIR = "data/raw"
OUT_DIR = "data/processed"


def detect_format() -> str:
    if os.path.exists(os.path.join(RAW_DIR, "fact_events.parquet")):
        return "parquet"
    if os.path.exists(os.path.join(RAW_DIR, "fact_events.csv")):
        return "csv"
    raise FileNotFoundError("No fact_events.parquet or fact_events.csv found in data/raw")


def read_table(name: str, fmt: str) -> pd.DataFrame:
    path = os.path.join(RAW_DIR, f"{name}.{fmt}")
    if fmt == "parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def save_table(df: pd.DataFrame, name: str, fmt: str) -> str:
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"{name}.{fmt}")
    if fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)
    return path


def dq_checks(events: pd.DataFrame, customers: pd.DataFrame, extras: pd.DataFrame) -> pd.DataFrame:
    today = pd.Timestamp.today().date()
    rows = []

    def add(check_name: str, table_name: str, severity: str, failed_rows: int):
        rows.append({
            "date": today,
            "check_name": check_name,
            "table_name": table_name,
            "severity": severity,
            "failed_rows": int(failed_rows),
            "sample_keys": ""
        })

    # 1) Missing keys
    missing = events[["event_ts", "event_date", "customer_id", "market", "extra_id", "event_type"]].isna().any(axis=1).sum()
    add("missing_keys", "events", "fail" if missing > 0 else "info", missing)

    # 2) Duplicates (same timestamp + customer + extra + event_type)
    dup_count = events.duplicated(subset=["event_ts", "customer_id", "extra_id", "event_type"]).sum()
    add("duplicates", "events", "warn" if dup_count > 0 else "info", dup_count)

    # 3) Invalid sequence: renew before purchase (per customer-market-extra)
    sub = events[events["event_type"].isin(["purchase", "renew"])].copy()
    if len(sub) == 0:
        invalid_seq = 0
    else:
        first_purchase = (sub[sub["event_type"] == "purchase"]
                         .groupby(["customer_id", "market", "extra_id"])["event_date"]
                         .min()
                         .rename("first_purchase"))
        first_renew = (sub[sub["event_type"] == "renew"]
                      .groupby(["customer_id", "market", "extra_id"])["event_date"]
                      .min()
                      .rename("first_renew"))
        seq = pd.concat([first_purchase, first_renew], axis=1).reset_index()
        invalid_seq = ((seq["first_renew"].notna()) &
                       ((seq["first_purchase"].isna()) | (seq["first_renew"] < seq["first_purchase"]))).sum()
    add("invalid_sequence_renew_before_purchase", "events", "warn" if invalid_seq > 0 else "info", invalid_seq)

    # 4) Market mismatch events vs customers
    merged = events[["customer_id", "market"]].merge(customers[["customer_id", "market"]], on="customer_id", how="left", suffixes=("_event", "_cust"))
    mismatch = (merged["market_cust"].notna() & (merged["market_event"] != merged["market_cust"])).sum()
    add("market_mismatch_events_vs_customers", "events/customers", "warn" if mismatch > 0 else "info", mismatch)

    # 5) Non-positive price
    nonpos = (extras["price_monthly"] <= 0).sum()
    add("non_positive_price", "extras", "fail" if nonpos > 0 else "info", nonpos)

    return pd.DataFrame(rows)


def build_gold_daily_kpi(events: pd.DataFrame, markets: pd.DataFrame, extras: pd.DataFrame) -> pd.DataFrame:
    # Ensure correct dtypes
    ev = events.copy()
    ev["event_date"] = pd.to_datetime(ev["event_date"]).dt.date

    # 1) Daily aggregated counts
    def count_type(df: pd.DataFrame, t: str) -> pd.Series:
        return (df["event_type"] == t).sum()

    daily = (ev
        .groupby(["event_date", "market", "extra_id"], as_index=False)
        .agg(
            trials=("event_type", lambda s: (s == "trial_start").sum()),
            purchases=("event_type", lambda s: (s == "purchase").sum()),
            renewals=("event_type", lambda s: (s == "renew").sum()),
            cancels=("event_type", lambda s: (s == "cancel").sum()),
            sessions=("event_type", lambda s: (s == "usage_session").sum()),
            active_users=("customer_id", lambda s: s[ev.loc[s.index, "event_type"] == "usage_session"].nunique()),
        )
    )
    daily = daily.rename(columns={"event_date": "date"})

    # 2) Add region/category/price
    out = (daily
        .merge(markets[["market", "region"]], on="market", how="left")
        .merge(extras[["extra_id", "category", "price_monthly"]], on="extra_id", how="left")
    )

    # 3) Active subscriptions proxy (cumulative net adds)
    out = out.sort_values(["market", "extra_id", "date"])
    out["net_adds"] = out["purchases"] + out["renewals"] - out["cancels"]
    out["active_subscriptions"] = out.groupby(["market", "extra_id"])["net_adds"].cumsum()

    # 4) MRR
    out["mrr"] = (out["active_subscriptions"] * out["price_monthly"]).round(2)

    # Keep final columns
    out = out[[
        "date","market","region","extra_id","category",
        "trials","purchases","renewals","cancels",
        "active_subscriptions","active_users","sessions","mrr"
    ]]

    return out


def build_gold_cohort_retention(events: pd.DataFrame) -> pd.DataFrame:
    ev = events.copy()
    ev["event_date"] = pd.to_datetime(ev["event_date"])
    ev["month"] = ev["event_date"].dt.to_period("M").dt.to_timestamp()

    # 1) Find first purchase month (cohort)
    purchases = ev[ev["event_type"] == "purchase"].copy()
    first_purchase = (purchases
        .groupby(["customer_id", "market", "extra_id"], as_index=False)["month"]
        .min()
        .rename(columns={"month": "cohort_month"})
    )

    # 2) Monthly active definition (simplified): any purchase or renew in that month
    monthly_active = ev[ev["event_type"].isin(["purchase", "renew"])][
        ["customer_id", "market", "extra_id", "month"]
    ].drop_duplicates()

    # 3) Join cohort with monthly activity
    j = first_purchase.merge(monthly_active, on=["customer_id", "market", "extra_id"], how="left")

    # month_n = months since cohort_month
    j["month_n"] = (j["month"].dt.to_period("M") - j["cohort_month"].dt.to_period("M")).apply(lambda x: x.n)

    j = j[j["month_n"] >= 0]

    # 4) cohort_size
    cohort_sizes = (first_purchase
        .groupby(["cohort_month", "market", "extra_id"], as_index=False)
        .size()
        .rename(columns={"size": "cohort_size"})
    )

    # 5) retained_subs per month_n
    retained = (j
        .groupby(["cohort_month", "market", "extra_id", "month_n"], as_index=False)["customer_id"]
        .nunique()
        .rename(columns={"customer_id": "retained_subs"})
    )

    out = retained.merge(cohort_sizes, on=["cohort_month", "market", "extra_id"], how="left")
    out["retention_rate"] = (out["retained_subs"] / out["cohort_size"]).round(4)

    # Ordering
    out = out.sort_values(["cohort_month", "market", "extra_id", "month_n"]).reset_index(drop=True)
    return out


def main() -> None:
    fmt = detect_format()

    markets = read_table("dim_market", fmt)
    extras = read_table("dim_extra", fmt)
    customers = read_table("dim_customer", fmt)
    events = read_table("fact_events", fmt)

    # DQ
    dq = dq_checks(events, customers, extras)
    print("DQ checks:\n", dq)

    # Gold KPI tables
    daily = build_gold_daily_kpi(events, markets, extras)
    cohorts = build_gold_cohort_retention(events)

    # Save
    p1 = save_table(dq, "gold_dq_results", fmt)
    p2 = save_table(daily, "gold_daily_kpi", fmt)
    p3 = save_table(cohorts, "gold_cohort_retention", fmt)

    print("\nSaved:")
    print("-", p1)
    print("-", p2)
    print("-", p3)


if __name__ == "__main__":
    main()