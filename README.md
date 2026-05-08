# 🤖 Automated Market Data Pipeline (Prefect Orchestration)

## 📌 Executive Summary
This project demonstrates enterprise-grade data workflow orchestration. It automates the extraction, transformation, and delivery of financial market data using a fault-tolerant Python pipeline managed by **Prefect**, the Modern Data Stack standard.

## 🏗️ Architecture & Features
* **Orchestration:** Built with Prefect `@flow` and `@task` decorators for complete observability and dependency management.
* **Fault Tolerance:** Engineered with automatic retries (`retries=2`, `retry_delay_seconds=5`) to handle simulated API rate limits and network degradation without crashing the system.
* **Data Processing:** Utilizes Pandas to clean mock API payloads and calculate market trends.
* **Alerting:** Automated terminal-based notification system simulating a push to executive Slack channels.

## ⚙️ Tech Stack
* **Orchestrator:** Prefect 3.x
* **Data Processing:** Python, Pandas
* **Environment:** Fully isolated virtual environment (PEP 668 compliance).

## 🚀 Business Impact (ROI)
Transforms manual, error-prone data pulls into a reliable, self-healing automated system that ensures stakeholders always have up-to-date metrics at the start of their day.