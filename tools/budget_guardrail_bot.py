#!/usr/bin/env python3
import os, sys, json, math, glob, datetime as dt
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "transactions"
CFG_PATH = ROOT / "config" / "budget_rules.yml"
DOCS_DIR = ROOT / "docs"
REPORT_MD = DOCS_DIR / "finance_report.md"
ALERTS_JSON = DOCS_DIR / "alerts.json"

DOCS_DIR.mkdir(parents=True, exist_ok=True)

def load_cfg():
    if not CFG_PATH.exists():
        raise SystemExit(f"Missing config at {CFG_PATH}")
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def load_transactions():
    files = sorted(glob.glob(str(DATA_DIR / "*.csv")))
    if not files:
        raise SystemExit(f"No CSVs found in {DATA_DIR}")
    dfs = []
    for fp in files:
        df = pd.read_csv(fp)
        cols = {c.lower().strip(): c for c in df.columns}

        # Normalize column names (best-effort across banks)
        date_col = next((cols[k] for k in cols if k in {"date", "booking date", "datum"}), None)
        amt_col  = next((cols[k] for k in cols if k in {"amount", "betrag", "value"}), None)
        desc_col = next((cols[k] for k in cols if k in {"description", "verwendungszweck", "description 1", "merchant"}), None)
        curr_col = next((cols[k] for k in cols if k in {"currency", "währung"}), None)
        acct_col = next((cols[k] for k in cols if k in {"account", "konto"}), None)

        if not (date_col and amt_col and desc_col):
            raise SystemExit(f"CSV {fp} missing required columns")

        tmp = pd.DataFrame({
            "date": pd.to_datetime(df[date_col], errors="coerce"),
            "amount": pd.to_numeric(df[amt_col], errors="coerce"),
            "description": df[desc_col].astype(str),
            "currency": (df[curr_col].astype(str) if curr_col else "N/A"),
            "account": (df[acct_col].astype(str) if acct_col else "N/A"),
            "source_file": os.path.basename(fp),
        }).dropna(subset=["date", "amount"])

        dfs.append(tmp)

    all_tx = pd.concat(dfs, ignore_index=True).sort_values("date")
    if all_tx.empty:
        raise SystemExit("No valid rows found in CSVs after parsing.")
    return all_tx

def categorize(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    rules = cfg.get("categorization", {}) or {}
    fallback = cfg.get("fallback_category", "Other")
    cats = []
    for desc in df["description"].astype(str):
        d = desc.upper()
        cat = None
        for c, patterns in rules.items():
            for p in (patterns or []):
                if p and p.upper() in d:
                    cat = c
                    break
            if cat:
                break
        cats.append(cat or fallback)
    out = df.copy()
    out["category"] = cats
    return out

def compute_kpis(df: pd.DataFrame, cfg: dict):
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    budgets = cfg.get("monthly_budgets", {}) or {}
    income_target = cfg.get("monthly_income_target", None)
    guard = cfg.get("guardrails", {})
    overspend_thr = float(guard.get("overspend_threshold", 1.10))
    min_savings_rate = float(guard.get("min_savings_rate", 0.2))
    min_runway = float(guard.get("min_runway_months", 6))

    # Identify income vs expense
    df["is_expense"] = df["amount"] < 0
    df["is_income"] = df["amount"] > 0

    # Current month window (Europe/Brussels)
    today = pd.Timestamp.now(tz="Europe/Brussels").normalize()
    this_month = today.to_period("M").to_timestamp()
    month_df = df[df["month"] == this_month]

    # Budgets tracking for current month
    spend_by_cat = (
        month_df[month_df["is_expense"]]
        .groupby("category")["amount"]
        .sum()
        .fillna(0)
        .abs()
    )
    rows = []
    alerts = []

    for cat, budget in budgets.items():
        spent = float(spend_by_cat.get(cat, 0.0))
        used = spent / max(float(budget), 1e-9)
        status = "OK"
        if used >= overspend_thr:
            status = "ALERT"
            alerts.append({
                "type": "overspend",
                "category": cat,
                "spent": round(spent, 2),
                "budget": float(budget),
                "ratio": round(used, 2),
            })
        rows.append({
            "category": cat,
            "budget": float(budget),
            "spent": round(spent, 2),
            "used_pct": round(100 * used, 1),
            "status": status,
        })

    # Savings rate and runway (based on last 3 fully closed months)
    closed = df[df["month"] < this_month]
    last3 = sorted(closed["month"].unique())[-3:]
    income_3, expense_3 = 0.0, 0.0
    if last3:
        recent = closed[closed["month"].isin(last3)]
        income_3 = recent[recent["is_income"]]["amount"].sum()
        expense_3 = -recent[recent["is_expense"]]["amount"].sum()

    avg_income = (income_3 / len(last3)) if last3 else (income_target or 0.0)
    avg_burn = (expense_3 / len(last3)) if last3 else 0.0
    net_month = avg_income - avg_burn
    savings_rate = (net_month / avg_income) if avg_income else 0.0

    if savings_rate < min_savings_rate:
        alerts.append({
            "type": "low_savings_rate",
            "savings_rate": round(100 * savings_rate, 1),
            "threshold_pct": round(100 * min_savings_rate, 1),
        })

    # Runway: approximate = current balances / avg_burn
    balances_csv = ROOT / "data" / "balances.csv"
    if balances_csv.exists():
        baldf = pd.read_csv(balances_csv)
        total_balance = float(baldf.get("balance", pd.Series(dtype=float)).sum())
    else:
        total_balance = float(df["amount"].cumsum().iloc[-1]) if not df.empty else 0.0

    runway_months = (total_balance / avg_burn) if avg_burn > 0 else math.inf
    if runway_months < min_runway:
        alerts.append({
            "type": "short_runway",
            "runway_months": round(runway_months, 1) if math.isfinite(runway_months) else "∞",
            "threshold_months": float(min_runway),
        })

    # Recurring subscriptions heuristic (top merchants charged 3+ months)
    expenses = closed[closed["is_expense"]].copy()
    expenses["merchant"] = expenses["description"].str.upper().str.replace(r"\s+", " ", regex=True)
    subs = (
        expenses
        .groupby(["merchant", "category", "month"])["amount"]
        .sum().abs().reset_index()
    )
    rec = (
        subs.groupby(["merchant", "category"])["month"]
            .nunique().reset_index(name="months_charged")
    )
    recurring = (
        rec[rec["months_charged"] >= 3]
        .sort_values("months_charged", ascending=False)
        .head(12)
    )

    # Unbudgeted categories with spend (help you update budgets)
    unbudgeted_spend = (
        month_df[month_df["is_expense"] & ~month_df["category"].isin(budgets)]
        .groupby("category")["amount"].sum().abs().sort_values(ascending=False)
    )

    kpis = {
        "month": str(this_month.date()),
        "budgets": rows,
        "avg_income": round(avg_income, 2),
        "avg_burn": round(avg_burn, 2),
        "net_month": round(net_month, 2),
        "savings_rate_pct": round(100 * savings_rate, 1),
        "runway_months": (round(runway_months, 1) if math.isfinite(runway_months) else "∞"),
        "top_recurring_merchants": recurring.to_dict(orient="records"),
        "alerts": alerts,
    }

    if not unbudgeted_spend.empty:
        kpis["unbudgeted_spend"] = [
            {"category": c, "spent": round(float(v), 2)} for c, v in unbudgeted_spend.items()
        ]

    return kpis, month_df

def write_report(kpis, month_df):
    md = []
    md.append(f"# 💰 Budget Guardrail Report — {kpis['month']}")
    md.append("")
    md.append(f"- **Avg Monthly Income (last 3 closed months):** €{kpis['avg_income']}")
    md.append(f"- **Avg Monthly Burn (last 3 closed months):** €{kpis['avg_burn']}")
    md.append(f"- **Net / mo:** €{kpis['net_month']} &nbsp;|&nbsp; **Savings rate:** {kpis['savings_rate_pct']}%")
    md.append(f"- **Runway:** {kpis['runway_months']} months")
    md.append("")
    md.append("## Budgets vs Spend (current month)")
    md.append("")
    md.append("| Category | Budget | Spent | Used | Status |")
    md.append("|---|---:|---:|---:|---|")
    for r in kpis["budgets"]:
        used = f"{r['used_pct']}%"
        status = "🟢 OK" if r["status"] == "OK" else "🔴 ALERT"
        md.append(f"| {r['category']} | €{r['budget']:.0f} | €{r['spent']:.2f} | {used} | {status} |")

    if kpis.get("top_recurring_merchants"):
        md.append("")
        md.append("## Recurring merchants (≥ 3 months)")
        md.append("")
        md.append("| Merchant | Category | Months billed |")
        md.append("|---|---|---:|")
        for row in kpis["top_recurring_merchants"]:
            md.append(f"| {row['merchant'].title()} | {row['category']} | {row['months_charged']} |")

    if kpis.get("unbudgeted_spend"):
        md.append("")
        md.append("## Unbudgeted categories with spend")
        md.append("")
        md.append("| Category | Spent |")
        md.append("|---|---:|")
        for row in kpis["unbudgeted_spend"]:
            md.append(f"| {row['category']} | €{row['spent']:.2f} |")

    if kpis["alerts"]:
        md.append("")
        md.append("## ⚠️ Alerts")
        for a in kpis["alerts"]:
            if a["type"] == "overspend":
                md.append(f"- **Overspend:** {a['category']} at €{a['spent']:.2f} / €{a['budget']:.0f} ({a['ratio']*100:.0f}% of budget)")
            elif a["type"] == "low_savings_rate":
                md.append(f"- **Low savings rate:** {a['savings_rate']}% (target ≥ {a['threshold_pct']}%)")
            elif a["type"] == "short_runway":
                md.append(f"- **Short runway:** {a['runway_months']} months (target ≥ {a['threshold_months']} months)")
    else:
        md.append("")
        md.append("## ✅ No alerts this cycle")

    REPORT_MD.write_text("\n".join(md), encoding="utf-8")
    ALERTS_JSON.write_text(json.dumps(kpis["alerts"], indent=2), encoding="utf-8")

def main():
    cfg = load_cfg()
    df = load_transactions()
    df = categorize(df, cfg)
    kpis, month_df = compute_kpis(df, cfg)
    write_report(kpis, month_df)
    print(json.dumps(kpis, indent=2))

if __name__ == "__main__":
    try:
        import pandas  # noqa: F401
    except Exception:
        print("Please install pandas: pip install pandas pyyaml", file=sys.stderr)
        sys.exit(1)
    main()
