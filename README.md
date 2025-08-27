# 💰 Budget Guardrail Bot

This project automatically analyzes your personal finance CSV exports, checks your budgets and savings guardrails, and generates weekly reports.  

It runs as a GitHub Action and commits results to the repo under `docs/`.

---

## How it works
- Place your bank exports in `data/transactions/bank_transactions.csv`.
- Configure budgets & guardrails in `config/budget_rules.yml`.
- Every Monday morning (cron), GitHub Actions runs:
  - Generates `docs/finance_report.md`
  - Generates `docs/alerts.json`
  - Fails CI if guardrails are breach
