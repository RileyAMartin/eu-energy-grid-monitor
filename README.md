# European Energy Grid Monitor

[View Live Dashboard](https://energy.rileymartin.me) | [Read the Engineering Process](https://energy.rileymartin.me/process.html)

A data engineering pipeline that ingests, processes, and visualizes real-time energy generation and pricing data from across Europe.

---

## Overview
The pipeline consists of three decoupled Python applications orchestrated via Docker:
1.  **Ingest App:** Polls the ENTSO-E API hourly, parses XML, and produces raw messages to Kafka.
2.  **Process App:** Consumes raw events, enriching them with carbon intensity data and mapping EIC codes to readable zones.
3.  **Storage App:** An idempotent sink service that persists enriched data to Cockroach DB.

## Tech Stack

* **Languages:** Python, SQL
* **Streaming:** Apache Kafka (Confluent Cloud)
* **Database:** CockroachDB (Serverless)
* **Infrastructure:** Docker, Oracle Cloud Free Tier (VM)
* **CI/CD:** GitHub Actions
* **Frontend:** HTML5, CSS3, Apache ECharts

## Getting Started

### Prerequisites

* Docker & Docker Compose
* Python 3.10+
* Access to a Kafka Cluster & Postgres Database

### Installation

1.  **Clone the repository**
    ```bash
    git clone [https://github.com/RileyAMartin/eu-energy-grid-monitor.git](https://github.com/RileyAMartin/eu-energy-grid-monitor.git)
    cd eu-energy-grid-monitor
    ```

2.  **Environment Setup**
    Create a `.env` file in the root directory with your credentials:
    ```env
    ENTSOE_API_KEY=your_key
    KAFKA_BOOTSTRAP_SERVERS=your_broker_url
    KAFKA_SASL_USERNAME=your_username
    KAFKA_SASL_PASSWORD=your_password
    DB_HOST=your_db_host
    DB_PORT=26257
    DB_NAME=your_db_name
    DB_USER=your_db_user
    DB_PASSWORD=your_db_password
    ```

3.  **Run with Docker Compose**
    ```bash
    docker-compose up --build
    ```

## Testing

The business logic is backed by `pytest`.

```bash
pip install -r requirements.txt
pytest
