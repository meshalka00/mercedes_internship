#!/usr/bin/env python3
"""
Generate synthetic but realistic dataset for "Digital Extras" analytics demo.

Outputs:
- dim_market
- dim_extra
- dim_customer
- fact_events

Event types:
- trial_start, purchase, renew, cancel, usage_session

Designed for demo purposes (no real customer data).
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Tuple, List

import numpy as np
import pandas as pd


# -----------------------------
# Config / helpers
# -----------------------------

@dataclass
class SaveResult:
    path: str
    fmt: str


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _try_save(df: pd.DataFrame, out_dir: str, name: str, preferred_fmt: str) -> SaveResult:
    """
    Save as parquet if possible; otherwise fallback to csv.
    """
    _ensure_dir(out_dir)
    preferred_fmt = preferred_fmt.lower()

    if preferred_fmt == "parquet":
        try:
            out_path = os.path.join(out_dir, f"{name}.parquet")
            df.to_parquet(out_path, index=False)
            return SaveResult(out_path, "parquet")
        except Exception:
            # fall back
            pass

    out_path = os.path.join(out_dir, f"{name}.csv")
    df.to_csv(out_path, index=False)
    return SaveResult(out_path, "csv")


def _date_range_days(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    return pd.date_range(start=start, end=end, freq="D")


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


# -----------------------------
# Data generation
# -----------------------------

def make_dim_market() -> pd.DataFrame:
    # Keep MVP size reasonable; you can scale to 50 later.
    markets_regions = [
        ("DE", "EU"), ("FR", "EU"), ("IT", "EU"), ("ES", "EU"), ("NL", "EU"),
        ("SE", "EU"), ("PL", "EU"), ("GB", "EU"),
        ("US", "NA"), ("CA", "NA"),
        ("BR", "LATAM"), ("MX", "LATAM"),
        ("AE", "MEA"), ("SA", "MEA"),
        ("IN", "APAC"), ("SG", "APAC"), ("JP", "APAC"), ("KR", "APAC"), ("AU", "APAC")
    ]
    return pd.DataFrame(markets_regions, columns=["market", "region"])


def make_dim_extra() -> pd.DataFrame:
    extras = [
        ("EX_01", "Navigation+", "Infotainment", 9.99),
        ("EX_02", "Remote Start", "Comfort", 5.99),
        ("EX_03", "Advanced Parking", "Safety", 7.99),
        ("EX_04", "Premium Audio", "Infotainment", 12.99),
        ("EX_05", "Driver Assist", "Safety", 14.99),
        ("EX_06", "Smart Charging", "EV", 6.99),
        ("EX_07", "Theft Alert", "Security", 4.99),
        ("EX_08", "Wi-Fi Hotspot", "Connectivity", 8.99),
    ]
    return pd.DataFrame(extras, columns=["extra_id", "extra_name", "category", "price_monthly"])


def make_dim_customer(
    rng: np.random.Generator,
    dim_market: pd.DataFrame,
    n_customers: int,
    start: pd.Timestamp,
    end: pd.Timestamp
) -> pd.DataFrame:
    markets = dim_market["market"].tolist()

    # Market weights: more customers in large markets (roughly)
    weights = np.array([
        0.07, 0.06, 0.05, 0.05, 0.03,
        0.02, 0.03, 0.04,
        0.20, 0.05,
        0.06, 0.04,
        0.03, 0.02,
        0.10, 0.02, 0.04, 0.03, 0.03
    ], dtype=float)
    weights = weights / weights.sum()

    chosen_markets = rng.choice(markets, size=n_customers, p=weights)

    segments = ["Private", "Business", "Premium"]
    seg_weights = np.array([0.70, 0.20, 0.10])
    chosen_segments = rng.choice(segments, size=n_customers, p=seg_weights)

    # Signup dates spread over the period (earlier bias)
    total_days = (end - start).days
    # Beta to bias signups earlier
    signup_offsets = (rng.beta(2.0, 5.0, size=n_customers) * total_days).astype(int)
    signup_dates = start + pd.to_timedelta(signup_offsets, unit="D")

    df = pd.DataFrame({
        "customer_id": [f"C_{i:06d}" for i in range(1, n_customers + 1)],
        "market": chosen_markets,
        "segment": chosen_segments,
        "signup_date": signup_dates.date
    })
    return df


def _market_base_rates(dim_market: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """
    Baseline rates by market:
    - trial_rate: probability that a customer starts at least one trial in the period
    - conv_uplift: multiplier for trial->purchase conversion (market effect)
    - churn_uplift: multiplier for churn probability (market effect)
    - usage_uplift: multiplier for usage intensity
    """
    rates = {}
    for _, row in dim_market.iterrows():
        m = row["market"]
        region = row["region"]

        # Heuristic region patterns (for meaningful insights)
        if region == "EU":
            trial_rate = 0.22
            conv_uplift = 1.05
            churn_uplift = 0.95
            usage_uplift = 1.00
        elif region == "NA":
            trial_rate = 0.26
            conv_uplift = 1.10
            churn_uplift = 0.90
            usage_uplift = 1.05
        elif region == "APAC":
            trial_rate = 0.20
            conv_uplift = 0.95
            churn_uplift = 1.05
            usage_uplift = 0.95
        elif region == "LATAM":
            trial_rate = 0.18
            conv_uplift = 0.85
            churn_uplift = 1.15
            usage_uplift = 0.90
        else:  # MEA
            trial_rate = 0.16
            conv_uplift = 0.90
            churn_uplift = 1.10
            usage_uplift = 0.90

        # Add small market-specific variation
        jitter = np.clip(np.random.normal(1.0, 0.05), 0.85, 1.20)
        rates[m] = {
            "trial_rate": float(np.clip(trial_rate * jitter, 0.08, 0.40)),
            "conv_uplift": float(np.clip(conv_uplift * jitter, 0.70, 1.35)),
            "churn_uplift": float(np.clip(churn_uplift * (2.0 - jitter), 0.75, 1.40)),
            "usage_uplift": float(np.clip(usage_uplift * jitter, 0.70, 1.35))
        }
    return rates


def _extra_base_rates(dim_extra: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """
    Baseline rates by extra:
    - base_conv: trial->purchase base probability
    - base_churn: monthly churn base probability
    - base_usage_lambda: expected daily usage sessions for active users
    """
    rates = {}
    for _, r in dim_extra.iterrows():
        ex = r["extra_id"]
        cat = r["category"]
        price = float(r["price_monthly"])

        # Heuristics: higher price -> lower conversion, higher churn risk
        base_conv = 0.18 - 0.004 * (price - 5.0)  # rough
        base_conv = float(np.clip(base_conv, 0.06, 0.22))

        base_churn = 0.06 + 0.002 * (price - 5.0)
        base_churn = float(np.clip(base_churn, 0.03, 0.10))

        if cat in ["Safety", "Security"]:
            base_usage = 0.20
        elif cat in ["Infotainment", "Connectivity"]:
            base_usage = 0.45
        elif cat in ["Comfort", "EV"]:
            base_usage = 0.30
        else:
            base_usage = 0.25

        rates[ex] = {
            "base_conv": base_conv,
            "base_churn": base_churn,
            "base_usage_lambda": float(base_usage)
        }
    return rates


def generate_events(
    rng: np.random.Generator,
    dim_market: pd.DataFrame,
    dim_extra: pd.DataFrame,
    dim_customer: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    campaign_date: pd.Timestamp,
    campaign_markets: List[str],
    campaign_conv_multiplier: float,
    quality_noise: float,
) -> pd.DataFrame:
    """
    Create event log with realistic lifecycle patterns:
    trial_start -> purchase -> renew (monthly) and/or cancel.
    usage_session events while trial/active.

    quality_noise: fraction of events to corrupt slightly (duplicates / invalid sequences) for DQ demo.
    """
    m_rates = _market_base_rates(dim_market)
    e_rates = _extra_base_rates(dim_extra)

    customers = dim_customer.copy()
    extras = dim_extra["extra_id"].tolist()
    all_days = _date_range_days(start, end)

    events_rows: List[Tuple] = []

    # Precompute per-customer: whether they start trials
    # Some customers will trial multiple extras.
    for _, c in customers.iterrows():
        cust = c["customer_id"]
        market = c["market"]
        signup = pd.Timestamp(c["signup_date"])
        if signup > end:
            continue

        trial_rate = m_rates[market]["trial_rate"]

        # Number of extras trialed depends on trial_rate
        if rng.random() > trial_rate:
            continue

        n_trials = int(np.clip(rng.poisson(1.2), 1, 4))
        trial_extras = rng.choice(extras, size=n_trials, replace=False)

        # Trial start dates after signup
        for ex in trial_extras:
            # pick trial start day
            min_day = max(signup, start)
            trial_start_day = min_day + pd.Timedelta(days=int(rng.integers(0, max(1, (end - min_day).days + 1))))
            # timestamp during day
            ts = trial_start_day + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
            events_rows.append((ts, ts.date(), cust, market, ex, "trial_start", 1))

            # Determine conversion to purchase
            base_conv = e_rates[ex]["base_conv"]
            conv = base_conv * m_rates[market]["conv_uplift"]

            # Campaign uplift after campaign_date in selected markets
            if (market in campaign_markets) and (trial_start_day >= campaign_date):
                conv *= campaign_conv_multiplier

            # Segment effect: Premium converts better, Business slightly better
            seg = c["segment"]
            if seg == "Premium":
                conv *= 1.20
            elif seg == "Business":
                conv *= 1.08

            conv = float(np.clip(conv, 0.02, 0.60))

            purchased = rng.random() < conv
            if not purchased:
                # usage during trial window (short)
                # simulate 3-14 days of trial usage
                trial_days = int(rng.integers(3, 15))
                usage_lambda = e_rates[ex]["base_usage_lambda"] * m_rates[market]["usage_uplift"]
                for d in range(trial_days):
                    day = trial_start_day + pd.Timedelta(days=d)
                    if day > end:
                        break
                    # sessions per day (0..)
                    n_sessions = rng.poisson(usage_lambda)
                    for _ in range(n_sessions):
                        uts = day + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
                        events_rows.append((uts, uts.date(), cust, market, ex, "usage_session", 1))
                continue

            # Purchase event: within 0-21 days from trial
            purchase_day = trial_start_day + pd.Timedelta(days=int(rng.integers(0, 22)))
            if purchase_day > end:
                continue
            pts = purchase_day + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
            events_rows.append((pts, pts.date(), cust, market, ex, "purchase", 1))

            # After purchase, generate renew/cancel monthly, plus usage
            active_start = purchase_day
            active_end = end

            monthly_churn = e_rates[ex]["base_churn"] * m_rates[market]["churn_uplift"]
            # Segment effect: Premium churns less
            if seg == "Premium":
                monthly_churn *= 0.80
            elif seg == "Business":
                monthly_churn *= 0.90
            monthly_churn = float(np.clip(monthly_churn, 0.01, 0.25))

            # Determine how many months subscription survives
            # Geometric with churn prob
            months_alive = 1
            while months_alive < 12 and rng.random() > monthly_churn:
                months_alive += 1

            # Trial-like early churn: some cancel in first month
            # months_alive already covers that; we also allow cancel day inside month.
            cancel_happens = True  # in this demo, most eventually cancel within horizon
            cancel_month = months_alive

            # Compute cancel date (approx month = 30 days)
            cancel_day = active_start + pd.Timedelta(days=30 * cancel_month) + pd.Timedelta(days=int(rng.integers(-5, 6)))
            if cancel_day > end:
                cancel_happens = False
                active_end = end
            else:
                active_end = cancel_day

            # Renew events each month boundary (1..months_alive-1) if within horizon
            for m in range(1, months_alive):
                renew_day = active_start + pd.Timedelta(days=30 * m)
                if renew_day > end:
                    break
                rts = renew_day + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
                events_rows.append((rts, rts.date(), cust, market, ex, "renew", 1))

            # Cancel event
            if cancel_happens:
                cts = active_end + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
                events_rows.append((cts, cts.date(), cust, market, ex, "cancel", 1))

            # Usage while active (daily)
            usage_lambda = e_rates[ex]["base_usage_lambda"] * m_rates[market]["usage_uplift"]
            day = active_start
            while day <= active_end and day <= end:
                n_sessions = rng.poisson(usage_lambda)
                for _ in range(n_sessions):
                    uts = day + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
                    events_rows.append((uts, uts.date(), cust, market, ex, "usage_session", 1))
                day += pd.Timedelta(days=1)

    events = pd.DataFrame(
        events_rows,
        columns=["event_ts", "event_date", "customer_id", "market", "extra_id", "event_type", "quantity"]
    )

    # Sort for readability
    events = events.sort_values(["event_ts", "customer_id", "extra_id"]).reset_index(drop=True)

    # Inject small data-quality noise for later DQ monitoring demo
    if quality_noise > 0 and len(events) > 0:
        n_noise = int(len(events) * quality_noise)

        # 50% duplicates: duplicate some random rows
        n_dup = n_noise // 2
        if n_dup > 0:
            dup_idx = rng.choice(events.index.to_numpy(), size=n_dup, replace=False)
            events = pd.concat([events, events.loc[dup_idx]], ignore_index=True)

        # 50% invalid sequences: create a few "renew" events before purchase (rare)
        n_inv = n_noise - n_dup
        if n_inv > 0:
            sample = events.sample(n=n_inv, random_state=42).copy()
            sample["event_type"] = "renew"
            sample["event_ts"] = sample["event_ts"] - pd.to_timedelta(rng.integers(1, 10, size=n_inv), unit="D")
            sample["event_date"] = sample["event_ts"].dt.date
            events = pd.concat([events, sample], ignore_index=True)

        events = events.sort_values(["event_ts", "customer_id", "extra_id"]).reset_index(drop=True)

    return events


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=str, default="data/raw", help="Output directory (default: data/raw)")
    ap.add_argument("--start", type=str, default="2025-01-01", help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, default="2025-12-31", help="End date YYYY-MM-DD")
    ap.add_argument("--n_customers", type=int, default=25000, help="Number of customers (default: 25000)")
    ap.add_argument("--seed", type=int, default=42, help="Random seed")
    ap.add_argument("--format", type=str, default="parquet", choices=["parquet", "csv"], help="Preferred output format")
    ap.add_argument("--campaign_date", type=str, default="2025-07-01", help="Campaign date YYYY-MM-DD")
    ap.add_argument("--campaign_conv_multiplier", type=float, default=1.25, help="Conversion multiplier after campaign")
    ap.add_argument("--quality_noise", type=float, default=0.002, help="Fraction of noisy events for DQ demo (default 0.2%)")
    args = ap.parse_args()

    start = pd.Timestamp(args.start)
    end = pd.Timestamp(args.end)
    if end < start:
        raise ValueError("end must be >= start")

    rng = _rng(args.seed)

    dim_market = make_dim_market()
    dim_extra = make_dim_extra()
    dim_customer = make_dim_customer(rng, dim_market, args.n_customers, start, end)

    # Campaign in a few markets (for clear insight)
    campaign_markets = ["DE", "FR", "US", "GB"]
    campaign_date = pd.Timestamp(args.campaign_date)

    fact_events = generate_events(
        rng=rng,
        dim_market=dim_market,
        dim_extra=dim_extra,
        dim_customer=dim_customer,
        start=start,
        end=end,
        campaign_date=campaign_date,
        campaign_markets=campaign_markets,
        campaign_conv_multiplier=args.campaign_conv_multiplier,
        quality_noise=args.quality_noise
    )

    # Save
    out_dir = args.out
    results = []
    results.append(_try_save(dim_market, out_dir, "dim_market", args.format))
    results.append(_try_save(dim_extra, out_dir, "dim_extra", args.format))
    results.append(_try_save(dim_customer, out_dir, "dim_customer", args.format))
    results.append(_try_save(fact_events, out_dir, "fact_events", args.format))

    # Quick summary
    print("\nGenerated tables:")
    for r in results:
        print(f"- {os.path.basename(r.path)} ({r.fmt})")

    print("\nRow counts:")
    print(f"dim_market:   {len(dim_market):,}")
    print(f"dim_extra:    {len(dim_extra):,}")
    print(f"dim_customer: {len(dim_customer):,}")
    print(f"fact_events:  {len(fact_events):,}")

    # Small sanity output
    print("\nEvent type distribution:")
    print(fact_events["event_type"].value_counts().to_string())


if __name__ == "__main__":
    main()
