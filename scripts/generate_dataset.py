#!/usr/bin/env python3
"""
Synthetic UAE retail-banking & payments dataset for the Azure data engineering project.

Everything here is FICTIONAL: names, merchants, IDs and e-mail addresses are randomly
generated (e-mails use example.com). Any resemblance to real people or companies is coincidence.

Usage:
    python generate_dataset.py --out ../data            # default sizes
    python generate_dataset.py --out ../data --seed 11  # different random data

Output layout (mirrors a raw/landing zone, so it can be uploaded to the ADLS Gen2 "landing" container as is):
    raw/customers/customers_initial.csv
    raw/customers/customer_changes.csv          (used later for the SCD Type 2 lab)
    raw/accounts/accounts.csv
    raw/merchants/merchants.jsonl               (nested JSON, one object per line)
    raw/fx_rates/fx_rates.csv
    raw/transactions/2026/MM/transactions_2026_MM.csv   (one file per month, Jan-Sep 2026)
    _manifest.json                              (row counts + injected data-quality issue counts = answer key)
"""
import argparse
import json
import os

import numpy as np
import pandas as pd

N_CUSTOMERS = 5000
N_MERCHANTS = 600
BASE_TXN_PER_MONTH = 22000
MONTHS = list(range(1, 10))  # Jan..Sep 2026
YEAR = 2026

FIRST = ["Omar", "Fatima", "Ahmed", "Mariam", "Khalid", "Aisha", "Yusuf", "Layla", "Hassan", "Noor",
         "Rahul", "Priya", "Amit", "Sneha", "Vikram", "Anjali", "Arjun", "Meera", "Imran", "Sana",
         "Maria", "Jose", "Grace", "Joel", "Angela", "Daniel", "Sarah", "James", "Emma", "Oliver",
         "Tariq", "Huda", "Karim", "Dina", "Bilal", "Rania", "Faisal", "Salma", "Zain", "Nadia"]
LAST = ["Al Mansoori", "Al Hashimi", "Khan", "Sharma", "Patel", "Iyer", "Nair", "Reddy", "Hussain",
        "Ali", "Santos", "Reyes", "Cruz", "Smith", "Brown", "Taylor", "Haddad", "Nasser", "Farouk",
        "Siddiqui", "Mehta", "Gupta", "Joseph", "Thomas", "Rahman", "Chowdhury", "Aziz", "Saleh",
        "Mirza", "Verma"]
NATIONALITIES = ["UAE", "India", "Pakistan", "Philippines", "Egypt", "UK", "Jordan", "Bangladesh", "Other"]
NAT_P = [0.25, 0.22, 0.10, 0.08, 0.07, 0.05, 0.04, 0.04, 0.15]
EMIRATES = ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah", "Fujairah", "Umm Al Quwain"]
EMI_P = [0.40, 0.25, 0.15, 0.07, 0.06, 0.04, 0.03]
CITY = {"Dubai": ["Dubai"], "Abu Dhabi": ["Abu Dhabi", "Al Ain"], "Sharjah": ["Sharjah"],
        "Ajman": ["Ajman"], "Ras Al Khaimah": ["Ras Al Khaimah"], "Fujairah": ["Fujairah"],
        "Umm Al Quwain": ["Umm Al Quwain"]}
SEGMENTS = ["Retail", "Premium", "SME"]
SEG_P = [0.70, 0.15, 0.15]

# (category, MCC, risk_tier, weight)
CATEGORIES = [
    ("Grocery", 5411, "Low", 18), ("Restaurants", 5812, "Low", 16), ("Fuel", 5541, "Low", 8),
    ("Electronics", 5732, "High", 6), ("Fashion", 5651, "Medium", 9), ("Airlines", 4511, "High", 4),
    ("Hotels", 7011, "Medium", 4), ("Healthcare", 8011, "Low", 5), ("Utilities", 4900, "Low", 5),
    ("Telecom", 4814, "Low", 5), ("Education", 8299, "Low", 3), ("Entertainment", 7832, "Medium", 4),
    ("Jewellery", 5944, "High", 3), ("Online Marketplace", 5964, "High", 10),
]
M_ADJ = ["Golden", "Blue", "Sunrise", "Desert", "Pearl", "Falcon", "Oasis", "Marina", "Palm", "Crescent",
         "Royal", "Prime", "Metro", "Harbour", "Skyline", "Coral"]
M_NOUN = {"Grocery": "Mart", "Restaurants": "Kitchen", "Fuel": "Fuel Station", "Electronics": "Electronics",
          "Fashion": "Boutique", "Airlines": "Air Travel", "Hotels": "Hotel", "Healthcare": "Clinic",
          "Utilities": "Utility Services", "Telecom": "Telecom", "Education": "Academy",
          "Entertainment": "Cinemas", "Jewellery": "Jewels", "Online Marketplace": "Online"}
FOREIGN = {"US": "USD", "GB": "GBP", "FR": "EUR", "DE": "EUR", "IN": "USD", "TR": "USD", "SA": "USD"}


def rand_dates(rng, start, end, n):
    s = pd.Timestamp(start).value // 10**9
    e = pd.Timestamp(end).value // 10**9
    return pd.to_datetime(rng.integers(s, e, n), unit="s").normalize()


def make_customers(rng):
    n = N_CUSTOMERS
    ids = [f"C{i:06d}" for i in range(1, n + 1)]
    first = rng.choice(FIRST, n)
    last = rng.choice(LAST, n)
    age = np.clip(rng.normal(37, 11, n), 18, 75).astype(int)
    dob = pd.Timestamp("2026-10-01") - pd.to_timedelta(age * 365 + rng.integers(0, 365, n), unit="D")
    emirate = rng.choice(EMIRATES, n, p=EMI_P)
    df = pd.DataFrame({
        "customer_id": ids,
        "first_name": first,
        "last_name": last,
        "date_of_birth": dob.strftime("%Y-%m-%d"),
        "gender": rng.choice(["F", "M"], n),
        "nationality": rng.choice(NATIONALITIES, n, p=NAT_P),
        "emirate": emirate,
        "segment": rng.choice(SEGMENTS, n, p=SEG_P),
        "kyc_status": rng.choice(["Verified", "Pending", "Expired"], n, p=[0.92, 0.06, 0.02]),
        "onboarding_date": rand_dates(rng, "2015-01-01", "2025-12-31", n).strftime("%Y-%m-%d"),
    })
    df["email"] = [f"{f.lower()}.{l.lower().replace(' ', '')}{i}@example.com"
                   for f, l, i in zip(df.first_name, df.last_name, range(1, n + 1))]
    return df


def make_customer_changes(rng, customers):
    """~15% of customers change segment or emirate during 2026 (feeds the SCD2 lab)."""
    pick = customers.sample(frac=0.15, random_state=int(rng.integers(0, 10**6))).reset_index(drop=True)
    rows = []
    for _, r in pick.iterrows():
        eff = rand_dates(rng, "2026-02-01", "2026-09-15", 1)[0].strftime("%Y-%m-%d")
        if rng.random() < 0.5:
            new = rng.choice([s for s in SEGMENTS if s != r.segment])
            rows.append((r.customer_id, "segment", r.segment, new, eff))
        else:
            new = rng.choice([e for e in EMIRATES if e != r.emirate])
            rows.append((r.customer_id, "emirate", r.emirate, new, eff))
    return pd.DataFrame(rows, columns=["customer_id", "attribute", "old_value", "new_value", "effective_date"]
                        ).sort_values(["effective_date", "customer_id"])


def make_accounts(rng, customers):
    rows = []
    n = 0
    for cid, onb in zip(customers.customer_id, customers.onboarding_date):
        k = rng.choice([1, 2, 3], p=[0.55, 0.33, 0.12])
        types = ["Current"] + list(rng.choice(["Savings", "Credit Card", "Wallet"], k - 1, replace=False))
        for t in types:
            n += 1
            open_dt = pd.Timestamp(onb) + pd.Timedelta(days=int(rng.integers(0, 200)))
            open_dt = min(open_dt, pd.Timestamp("2025-12-31"))
            rows.append((f"A{n:08d}", cid, t, rng.choice(["AED", "USD", "EUR"], p=[0.90, 0.07, 0.03]),
                         open_dt.strftime("%Y-%m-%d"),
                         rng.choice(["Active", "Dormant", "Closed"], p=[0.94, 0.04, 0.02]),
                         f"BR{int(rng.integers(1, 41)):03d}"))
    return pd.DataFrame(rows, columns=["account_id", "customer_id", "account_type", "currency",
                                       "open_date", "status", "branch_code"])


def make_merchants(rng):
    cats = [c[0] for c in CATEGORIES]
    w = np.array([c[3] for c in CATEGORIES], dtype=float)
    w /= w.sum()
    meta = {c[0]: c for c in CATEGORIES}
    rows = []
    for i in range(1, N_MERCHANTS + 1):
        cat = rng.choice(cats, p=w)
        emi = rng.choice(EMIRATES, p=EMI_P)
        rows.append({
            "merchant_id": f"M{i:05d}",
            "name": f"{rng.choice(M_ADJ)} {M_NOUN[cat]} {i}",
            "mcc": int(meta[cat][1]),
            "category": cat,
            "location": {"emirate": emi, "city": str(rng.choice(CITY[emi]))},
            "risk_tier": meta[cat][2],
            "onboarded_date": rand_dates(rng, "2016-01-01", "2025-12-31", 1)[0].strftime("%Y-%m-%d"),
        })
    return rows


def make_fx(rng):
    days = pd.date_range(f"{YEAR}-01-01", f"{YEAR}-09-30", freq="D")
    eur = 4.25 + np.cumsum(rng.normal(0, 0.012, len(days)))
    gbp = 4.90 + np.cumsum(rng.normal(0, 0.014, len(days)))
    out = []
    for d, e, g in zip(days, eur, gbp):
        out += [(d.strftime("%Y-%m-%d"), "USD", 3.6725),  # AED is pegged to USD
                (d.strftime("%Y-%m-%d"), "EUR", round(float(e), 4)),
                (d.strftime("%Y-%m-%d"), "GBP", round(float(g), 4))]
    return pd.DataFrame(out, columns=["rate_date", "currency", "rate_to_aed"])


def make_month(rng, month, accounts, merchants_df, counter_start, manifest):
    n = int(BASE_TXN_PER_MONTH * (1 + 0.03 * (month - 1)))
    elig = accounts[(accounts.status != "Closed")].reset_index(drop=True)
    w = np.where(elig.status == "Active", 1.0, 0.05)
    w = w / w.sum()
    acc_idx = rng.choice(len(elig), n, p=w)
    acc = elig.iloc[acc_idx].reset_index(drop=True)

    types = rng.choice(["PURCHASE", "BILL_PAY", "TRANSFER_OUT", "ATM_WITHDRAWAL", "REFUND", "WALLET_TOPUP"],
                       n, p=[0.62, 0.08, 0.12, 0.09, 0.04, 0.05])
    channel = np.empty(n, dtype=object)
    for t, ch, p in [("PURCHASE", ["POS", "ECOM", "WALLET"], [0.55, 0.35, 0.10]),
                     ("BILL_PAY", ["MOBILE_APP", "ECOM"], [0.7, 0.3]),
                     ("TRANSFER_OUT", ["MOBILE_APP", "BRANCH"], [0.8, 0.2]),
                     ("ATM_WITHDRAWAL", ["ATM"], [1.0]),
                     ("REFUND", ["POS", "ECOM"], [0.5, 0.5]),
                     ("WALLET_TOPUP", ["MOBILE_APP"], [1.0])]:
        m = types == t
        channel[m] = rng.choice(ch, m.sum(), p=p)

    # merchants
    merch = np.full(n, None, dtype=object)
    util_ids = merchants_df[merchants_df.category.isin(["Utilities", "Telecom"])].merchant_id.values
    shop = merchants_df[~merchants_df.category.isin(["Utilities", "Telecom"])].merchant_id.values
    m = np.isin(types, ["PURCHASE", "REFUND"])
    merch[m] = rng.choice(shop, m.sum())
    m = types == "BILL_PAY"
    merch[m] = rng.choice(util_ids, m.sum())

    # amounts
    amt = np.zeros(n)
    for t, mu, sg in [("PURCHASE", 4.3, 1.0), ("BILL_PAY", 5.2, 0.6), ("TRANSFER_OUT", 6.2, 1.1), ("REFUND", 4.0, 1.0)]:
        m = types == t
        amt[m] = rng.lognormal(mu, sg, m.sum())
    m = types == "ATM_WITHDRAWAL"
    amt[m] = rng.choice([100, 200, 300, 500, 1000, 2000], m.sum())
    m = types == "WALLET_TOPUP"
    amt[m] = rng.choice([50, 100, 200, 500, 1000], m.sum())
    amt = np.round(np.clip(amt, 1, 60000), 2)

    # country / currency
    country = np.full(n, "AE", dtype=object)
    currency = acc.currency.values.astype(object)
    foreign_mask = (np.isin(channel, ["ECOM", "POS"])) & (rng.random(n) < 0.09)
    fc = rng.choice(list(FOREIGN.keys()), foreign_mask.sum())
    country[foreign_mask] = fc
    currency[foreign_mask] = [FOREIGN[c] for c in fc]

    # timestamps (UAE local time, Gulf Standard Time UTC+4)
    start = pd.Timestamp(YEAR, month, 1)
    days_in = (start + pd.offsets.MonthEnd(0)).day
    hw = np.array([1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 7, 8, 7, 6, 6, 7, 8, 9, 9, 7, 5, 3, 2], dtype=float)
    hour = rng.choice(24, n, p=hw / hw.sum())
    ts = (start + pd.to_timedelta(rng.integers(0, days_in, n), unit="D")
          + pd.to_timedelta(hour, unit="h") + pd.to_timedelta(rng.integers(0, 3600, n), unit="s"))

    # status
    status = rng.choice(["SUCCESS", "DECLINED", "REVERSED"], n, p=[0.955, 0.035, 0.010])
    reason = np.full(n, None, dtype=object)
    dm = status == "DECLINED"
    reason[dm] = rng.choice(["INSUFFICIENT_FUNDS", "SUSPECTED_FRAUD", "LIMIT_EXCEEDED", "CARD_EXPIRED", "INVALID_PIN"],
                            dm.sum(), p=[0.50, 0.20, 0.15, 0.10, 0.05])

    # fraud flag (label)
    night = (hour <= 4)
    p = 0.002 + 0.02 * foreign_mask + 0.01 * night + 0.01 * (amt > 2000)
    fraud = (rng.random(n) < p).astype(int)
    fraud[reason == "SUSPECTED_FRAUD"] = (rng.random((reason == "SUSPECTED_FRAUD").sum()) < 0.6).astype(int)

    ids = [f"TXN{counter_start + i:09d}" for i in range(n)]
    df = pd.DataFrame({
        "transaction_id": ids,
        "account_id": acc.account_id.values,
        "merchant_id": merch,
        "txn_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "txn_type": types,
        "channel": channel,
        "amount": amt,
        "currency": currency,
        "country": country,
        "status": status,
        "decline_reason": reason,
        "fraud_flag": fraud,
    })

    # ---------------- inject data-quality issues (answer key goes to the manifest) ----------------
    issues = {}

    def pick(frac):
        return rng.choice(n, max(1, int(n * frac)), replace=False)

    # late-arriving rows: timestamps from the last 4 days of the previous month
    if month > 1:
        idx = pick(0.012)
        prev_end = start - pd.Timedelta(days=1)
        late_ts = (prev_end.normalize() - pd.to_timedelta(rng.integers(0, 4, len(idx)), unit="D")
                   + pd.to_timedelta(rng.integers(0, 86400, len(idx)), unit="s"))
        df.loc[idx, "txn_timestamp"] = late_ts.strftime("%Y-%m-%d %H:%M:%S")
        issues["late_arriving_rows"] = len(idx)
    # missing amount
    idx = pick(0.0015)
    df.loc[idx, "amount"] = np.nan
    issues["missing_amount"] = len(idx)
    # negative purchase amount
    cand = df.index[(df.txn_type == "PURCHASE") & df.amount.notna()]
    idx = rng.choice(cand, max(1, int(n * 0.001)), replace=False)
    df.loc[idx, "amount"] = -df.loc[idx, "amount"]
    issues["negative_purchase_amount"] = len(idx)
    # currency code case / whitespace
    idx = pick(0.003)
    df.loc[idx, "currency"] = df.loc[idx, "currency"].str.lower()
    issues["currency_lowercase"] = len(idx)
    idx = pick(0.001)
    df.loc[idx, "currency"] = df.loc[idx, "currency"] + " "
    issues["currency_trailing_space"] = len(idx)
    # orphan merchant / account keys
    cand = df.index[df.merchant_id.notna()]
    idx = rng.choice(cand, max(1, int(n * 0.002)), replace=False)
    df.loc[idx, "merchant_id"] = "M99999"
    issues["orphan_merchant_id"] = len(idx)
    idx = pick(0.0005)
    df.loc[idx, "account_id"] = "A99999999"
    issues["orphan_account_id"] = len(idx)
    # alternative timestamp format dd/mm/yyyy HH:MM
    idx = pick(0.001)
    parsed = pd.to_datetime(df.loc[idx, "txn_timestamp"])
    df.loc[idx, "txn_timestamp"] = parsed.dt.strftime("%d/%m/%Y %H:%M")
    issues["timestamp_ddmmyyyy_format"] = len(idx)
    # exact duplicate rows (appended at random positions)
    idx = pick(0.004)
    dups = df.loc[idx]
    df = pd.concat([df, dups]).sample(frac=1, random_state=int(rng.integers(0, 10**6))).reset_index(drop=True)
    issues["exact_duplicate_rows"] = len(idx)

    # Answer key = what is actually IN the file after duplication (a duplicated row that also carries
    # another issue counts for both copies), measured with the same detectors you would use yourself.
    ts1 = pd.to_datetime(df.txn_timestamp, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    ts2 = pd.to_datetime(df.txn_timestamp, format="%d/%m/%Y %H:%M", errors="coerce")
    tsx = ts1.fillna(ts2)
    merchant_ids = set(merchants_df.merchant_id)
    issues = {
        "exact_duplicate_rows": int(df.duplicated().sum()),
        "late_arriving_rows": int((tsx.notna() & (tsx.dt.month != month)).sum()),
        "missing_amount": int(df.amount.isna().sum()),
        "negative_purchase_amount": int(((df.txn_type == "PURCHASE") & (df.amount < 0)).sum()),
        "currency_lowercase": int(df.currency.str.islower().sum()),
        "currency_trailing_space": int(df.currency.str.endswith(" ").sum()),
        "orphan_merchant_id": int((df.merchant_id.notna() & ~df.merchant_id.isin(merchant_ids)).sum()),
        "orphan_account_id": int((~df.account_id.isin(accounts.account_id)).sum()),
        "timestamp_ddmmyyyy_format": int(df.txn_timestamp.str.contains("/", regex=False).sum()),
    }

    manifest["transactions"][f"{YEAR}-{month:02d}"] = {"rows_in_file": int(len(df)),
                                                         "unique_transaction_ids": int(n),
                                                         "issues": issues}
    return df, counter_start + n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="../data")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    rng = np.random.default_rng(a.seed)
    root = os.path.join(a.out, "raw")
    manifest = {"seed": a.seed, "note": "Answer key. Try to find the issues yourself before reading.",
                "transactions": {}}

    customers = make_customers(rng)
    changes = make_customer_changes(rng, customers)
    accounts = make_accounts(rng, customers)
    merchants = make_merchants(rng)
    merchants_df = pd.DataFrame([{"merchant_id": m["merchant_id"], "category": m["category"]} for m in merchants])
    fx = make_fx(rng)

    for sub in ["customers", "accounts", "merchants", "fx_rates"]:
        os.makedirs(os.path.join(root, sub), exist_ok=True)
    customers.to_csv(os.path.join(root, "customers", "customers_initial.csv"), index=False)
    changes.to_csv(os.path.join(root, "customers", "customer_changes.csv"), index=False)
    accounts.to_csv(os.path.join(root, "accounts", "accounts.csv"), index=False)
    with open(os.path.join(root, "merchants", "merchants.jsonl"), "w", encoding="utf-8") as f:
        for m in merchants:
            f.write(json.dumps(m) + "\n")
    fx.to_csv(os.path.join(root, "fx_rates", "fx_rates.csv"), index=False)

    manifest.update({"customers": len(customers), "customer_changes": len(changes), "accounts": len(accounts),
                     "merchants": len(merchants), "fx_rates": len(fx)})

    counter = 1
    for mth in MONTHS:
        df, counter = make_month(rng, mth, accounts, merchants_df, counter, manifest)
        d = os.path.join(root, "transactions", str(YEAR), f"{mth:02d}")
        os.makedirs(d, exist_ok=True)
        df.to_csv(os.path.join(d, f"transactions_{YEAR}_{mth:02d}.csv"), index=False)

    manifest["total_transaction_rows_in_files"] = sum(v["rows_in_file"] for v in manifest["transactions"].values())
    with open(os.path.join(a.out, "_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    print(json.dumps({k: v for k, v in manifest.items() if k != "transactions"}, indent=2))
    print("Jan 2026:", manifest["transactions"]["2026-01"])


if __name__ == "__main__":
    main()
