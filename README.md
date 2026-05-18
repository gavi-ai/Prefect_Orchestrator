# Automated Market Data Pipeline — Prefect Orchestration

Daily financial data extraction and transformation workflow built with 
Prefect 3.x. Handles API failures automatically, runs on a schedule, 
and delivers pipeline completion alerts to Slack — without manual 
intervention.

---

## What this solves

Manual daily data pulls are fragile — one failed API call, one missed 
run, and stakeholders have stale numbers. This pipeline automates the 
full cycle with retry logic, observability, and alerting baked in.

---

## How it works
[Prefect Flow: daily_market_pipeline]
│
├── fetch_market_data()        # Pulls financial data from mock API
│     retries=2, delay=5s      # Auto-recovers from rate limits / timeouts
│
├── transform_data()           # Pandas: cleans payload, calculates trends
│
├── generate_report()          # Structures output for downstream consumption
│
└── send_alert()               # Slack notification on completion

Full run observability via Prefect Cloud dashboard — every task state, 
duration, and failure is logged.

---

## Tech stack

| Component | Tool |
|---|---|
| Orchestration | Prefect 3.x (`@flow`, `@task`) |
| Data processing | Python, Pandas |
| Alerting | Slack API (simulated) |
| CI/CD | GitHub Actions + flake8 |

---

## Repository structure
Prefect_Orchestrator/
├── src/
│   ├── pipeline.py        # Main Prefect flow definition
│   ├── fetch.py           # Data extraction tasks
│   ├── transform.py       # Pandas transformation logic
│   └── alerts.py          # Slack alert task
├── .github/
│   └── workflows/
│       └── ci.yml         # Lint on every push
├── requirements.txt
└── README.md

---

## Running locally

```bash
pip install -r requirements.txt

# Run the pipeline once
python src/pipeline.py

# Deploy to Prefect Cloud (optional)
prefect deployment build src/pipeline.py:daily_market_pipeline -n "daily-run"
prefect deployment apply daily_market_pipeline-deployment.yaml
```

---

## Fault tolerance

Prefect tasks are configured with `retries=2, retry_delay_seconds=5`. 
If the API returns a rate limit error or the network drops, the task 
retries automatically before marking as failed — no manual restart needed.

---

## CI/CD

Every push triggers a GitHub Actions workflow that lints all source 
files with flake8. Catches style errors and syntax issues before they 
reach main.
