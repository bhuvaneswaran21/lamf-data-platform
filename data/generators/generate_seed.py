import argparse
import csv
import os
import random
import uuid
from datetime import date, datetime, timedelta

from faker import Faker

CATEGORIES = {
    "EQUITY": (50.0, (50, 800), 0.018),
    "HYBRID": (60.0, (20, 400), 0.011),
    "DEBT":   (75.0, (10, 60),  0.003),
}
AMCS = ["HDFC", "ICICI", "SBI", "AXIS", "KOTAK", "NIPPON", "UTI", "ADITYA_BIRLA"]
RTAS = ["CAMS", "KFINTECH"]


def daterange(start: date, days: int):
    for i in range(days):
        d = start + timedelta(days=i)
        if d.weekday() < 5:
            yield d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=1000)
    ap.add_argument("--schemes", type=int, default=200)
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--stress", type=str, default=None,
                    help="YYYY-MM-DD to apply a market shock (~ -12%)")
    ap.add_argument("--out", type=str, default="../generated")
    args = ap.parse_args()

    random.seed(args.seed)
    fake = Faker("en_IN")
    Faker.seed(args.seed)
    os.makedirs(args.out, exist_ok=True)
    stress_day = datetime.strptime(args.stress, "%Y-%m-%d").date() if args.stress else None
    start = date.today() - timedelta(days=args.days)

    schemes = []
    for i in range(args.schemes):
        cat = random.choice(list(CATEGORIES))
        ltv, (lo, hi), vol = CATEGORIES[cat]
        base_nav = round(random.uniform(lo, hi), 4)
        schemes.append({
            "scheme_code": f"{100000 + i}",
            "isin": f"INF{random.randint(100,999)}K01{fake.lexify('???').upper()}{i%10}",
            "scheme_name": f"{random.choice(AMCS)} {cat.title()} Fund - Growth",
            "amc": random.choice(AMCS),
            "category": cat,
            "eligible_ltv_pct": ltv,
            "_base_nav": base_nav,
            "_vol": vol,
        })
    _write(args.out, "schemes.csv",
           ["scheme_code", "isin", "scheme_name", "amc", "category", "eligible_ltv_pct"],
           [{k: s[k] for k in ["scheme_code", "isin", "scheme_name", "amc", "category", "eligible_ltv_pct"]} for s in schemes])

    nav_rows = []
    for s in schemes:
        nav = s["_base_nav"]
        for d in daterange(start, args.days):
            shock = -0.12 if (stress_day and d == stress_day) else 0.0
            nav = max(1.0, nav * (1 + random.gauss(0, s["_vol"]) + shock))
            nav_rows.append({
                "scheme_code": s["scheme_code"], "isin": s["isin"],
                "nav": round(nav, 4), "nav_date": d.isoformat(),
                "published_at": f"{d.isoformat()}T23:30:00+05:30",
            })
    _write(args.out, "nav_history.csv",
           ["scheme_code", "isin", "nav", "nav_date", "published_at"], nav_rows)
    latest_nav = {r["scheme_code"]: r["nav"] for r in nav_rows}  

    customers = []
    for _ in range(args.customers):
        cid = str(uuid.uuid4())
        customers.append({
            "customer_id": cid,
            "full_name": fake.name(),
            "pan": fake.bothify("?????####?").upper(),
            "email": fake.email(),
            "phone": fake.msisdn()[:10],
            "dob": fake.date_of_birth(minimum_age=23, maximum_age=65).isoformat(),
            "kyc_status": random.choices(["VERIFIED", "PENDING", "REJECTED"], [0.9, 0.07, 0.03])[0],
            "kyc_type": random.choice(["CKYC", "EKYC", "VIDEO"]),
            "risk_category": random.choices(["LOW", "MEDIUM", "HIGH"], [0.6, 0.3, 0.1])[0],
            "city": fake.city(), "state": fake.state(), "pincode": fake.postcode(),
            "created_at": fake.date_time_between(start_date="-2y").isoformat(),
        })
    _write(args.out, "customers.csv", list(customers[0].keys()), customers)

    holdings, loans, disb, repay = [], [], [], []
    today = date.today()
    for c in customers:
        if c["kyc_status"] != "VERIFIED" or random.random() > 0.8:
            continue  
        n_hold = random.randint(1, 4)
        collateral, dp = 0.0, 0.0
        for _ in range(n_hold):
            s = random.choice(schemes)
            units = round(random.uniform(50, 5000), 4)
            nav = latest_nav[s["scheme_code"]]
            collateral += units * nav
            dp += units * nav * (s["eligible_ltv_pct"] / 100.0)
            holdings.append({
                "holding_id": str(uuid.uuid4()), "customer_id": c["customer_id"],
                "folio_number": fake.bothify("########/##"),
                "scheme_code": s["scheme_code"], "isin": s["isin"],
                "units_pledged": units, "units_total": round(units * random.uniform(1.0, 1.5), 4),
                "rta": random.choice(RTAS), "lien_status": "MARKED",
                "lien_ref": fake.bothify("LIEN-########"), "as_of_date": today.isoformat(),
            })

        r = random.random()
        if r < 0.05:
            util = random.uniform(1.01, 1.12)
        elif r < 0.20:
            util = random.uniform(0.85, 0.99)
        else:
            util = random.uniform(0.20, 0.80)
        outstanding = round(dp * util, 2)
        loan_id = str(uuid.uuid4())
        disbursed_at = fake.date_time_between(start_date="-1y")
        loans.append({
            "loan_id": loan_id, "customer_id": c["customer_id"], "product_type": "LAMF_OD",
            "sanctioned_limit": round(dp, 2), "outstanding": outstanding,
            "applied_ltv_pct": 50.0, "status": "DISBURSED",
            "annual_rate": round(random.uniform(9.5, 13.5), 2),
            "disbursed_at": disbursed_at.isoformat(),
        })
        disb.append({
            "disbursement_id": str(uuid.uuid4()), "loan_id": loan_id,
            "customer_id": c["customer_id"], "amount": outstanding,
            "utr": fake.bothify("UTR############"),
            "disbursed_at": disbursed_at.isoformat(), "status": "SUCCESS",
        })
        # repayments; some overdue to create DPD buckets
        for _ in range(random.randint(0, 6)):
            amt = round(outstanding * random.uniform(0.02, 0.1), 2)
            paid = fake.date_time_between(start_date=disbursed_at)
            repay.append({
                "repayment_id": str(uuid.uuid4()), "loan_id": loan_id,
                "customer_id": c["customer_id"], "amount": amt,
                "principal_comp": round(amt * 0.8, 2), "interest_comp": round(amt * 0.2, 2),
                "mode": random.choice(["UPI", "NETBANKING", "ENACH"]),
                "paid_at": paid.isoformat(),
                "status": random.choices(["SUCCESS", "FAILED"], [0.95, 0.05])[0],
            })

    _write(args.out, "holdings.csv", list(holdings[0].keys()), holdings)
    _write(args.out, "loans.csv", list(loans[0].keys()), loans)
    _write(args.out, "disbursements.csv", list(disb[0].keys()), disb)
    _write(args.out, "repayments.csv", list(repay[0].keys()), repay)

    print(f"Generated: {len(customers)} customers, {len(schemes)} schemes, "
          f"{len(nav_rows)} nav rows, {len(holdings)} holdings, {len(loans)} loans, "
          f"{len(disb)} disbursements, {len(repay)} repayments -> {args.out}")


def _write(out, name, fields, rows):
    path = os.path.join(out, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in fields})


if __name__ == "__main__":
    main()
