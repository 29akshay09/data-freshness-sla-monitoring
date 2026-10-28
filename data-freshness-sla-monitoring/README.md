# Data Freshness and SLA Monitoring System

A Python and PostgreSQL-based data engineering project designed to monitor data freshness, measure ingestion latency, and identify pipeline Service Level Agreement (SLA) violations.

The goal of this project is to help data engineers detect stale datasets, monitor data pipeline health, and improve the reliability of data processing workflows.

## Project Overview

Data pipelines can fail silently, deliver outdated records, or miss their expected delivery deadlines. These issues can affect dashboards, reports, and downstream applications.

This project provides a foundation for monitoring data freshness and ingestion latency while tracking pipeline execution status and SLA compliance.

### Key Objectives

* Generate sample transaction data for pipeline testing.
* Store transaction records in PostgreSQL.
* Monitor the freshness of incoming data.
* Calculate event-to-ingestion latency.
* Check pipeline execution against configured SLA deadlines.
* Provide a foundation for automated alerts and monitoring dashboards.

## Architecture

```text
                  Sample Transaction Data
                           |
                           v
                  Data Generator
                  data_generator.py
                           |
                           v
                   Data Ingestion
                  data_ingestion.py
                           |
                           v
                      PostgreSQL
                     transactions
                           |
              +------------+------------+
              |                         |
              v                         v
       Freshness Checker         Latency Calculator
       freshness_checker.py      latency_calculator.py
              |                         |
              v                         v
       Data Age Status           Ingestion Latency
              |                         |
              +------------+------------+
                           |
                           v
                     SLA Checker
                     sla_checker.py
                           |
                           v
                  Pipeline Monitoring
```

*Note: This diagram represents the current project direction. Automated orchestration, alerting, and dashboard integration can be added as those components are implemented.*

## Technology Stack

| Technology     | Purpose                                              |
| -------------- | ---------------------------------------------------- |
| Python         | Monitoring logic and data processing                 |
| PostgreSQL     | Transaction storage and monitoring metadata          |
| SQL            | Data retrieval, aggregation, and database operations |
| Psycopg        | Python connectivity to PostgreSQL                    |
| python-dotenv  | Loading environment variables                        |
| Docker         | Running PostgreSQL locally using containers          |
| Git and GitHub | Version control and project hosting                  |
| Apache Airflow | Planned pipeline scheduling and orchestration        |
| Grafana        | Planned monitoring dashboards and visualizations     |

## Project Structure

```text
data-freshness-sla-monitoring/
│
├── src/
│   ├── __init__.py
│   ├── data_generator.py
│   ├── data_ingestion.py
│   ├── freshness_checker.py
│   ├── latency_calculator.py
│   ├── sla_checker.py
│   └── alert_manager.py
│
├── sql/
│   └── create_tables.sql
│
├── dags/
│
├── tests/
│
├── config/
│
├── monitoring/
│
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
└── README.md
```

The structure above is the intended layout. Keep only files and components that exist in your repository, and update the tree as the project evolves.

## Features

### 1. Sample Data Generation

The data generator creates synthetic transaction records for testing.

Example fields include:

* Transaction ID
* Customer ID
* Transaction amount
* Event timestamp

This allows the monitoring system to be tested without requiring a production dataset.

### 2. Data Ingestion

The ingestion component loads generated transaction records into PostgreSQL.

The database stores event timestamps and ingestion timestamps, allowing the project to measure how much time passes between an event occurring and its arrival in the database.

### 3. Data Freshness Monitoring

The freshness checker retrieves the latest event timestamp and calculates the age of the most recent data.

Conceptually:

`Data Freshness = Current Time - Latest Event Timestamp`

The current implementation uses a 15-minute freshness threshold.

| Condition                        | Interpretation    |
| -------------------------------- | ----------------- |
| Freshness at or below 15 minutes | HEALTHY           |
| Freshness above 15 minutes       | STALE             |
| No available timestamp           | NO_DATA           |
| Timestamp in the future          | INVALID_TIMESTAMP |

These statuses describe the data's freshness, not the duration of the pipeline's execution.

### 4. Ingestion Latency Monitoring

The latency calculator measures the difference between an event timestamp and its ingestion timestamp.

`Ingestion Latency = Ingested At - Event Timestamp`

The report calculates:

* Total transactions
* Average ingestion latency
* Maximum ingestion latency
* Minimum ingestion latency

These metrics help identify delayed data arrival.

Because the sample generator creates events with different timestamps, the calculated latency may vary considerably.

### 5. SLA Monitoring

The SLA checker reads pipeline execution records from the `pipeline_runs` table.

It compares a pipeline's recorded completion time with its configured SLA deadline.

| Result   | Meaning                               |
| -------- | ------------------------------------- |
| MET      | Pipeline completed by its deadline    |
| BREACHED | Pipeline completed after its deadline |
| RUNNING  | Completion time has not been recorded |
| UNKNOWN  | No SLA deadline is configured         |

This component relies on pipeline-run records being present in PostgreSQL.

## Database Design

The project uses PostgreSQL to store transaction data and pipeline monitoring metadata.

### `transactions`

Stores sample transaction records.

| Column            | Purpose                              |
| ----------------- | ------------------------------------ |
| `transaction_id`  | Unique transaction identifier        |
| `customer_id`     | Customer identifier                  |
| `amount`          | Transaction amount                   |
| `event_timestamp` | Time the event occurred              |
| `ingested_at`     | Time the record entered the database |

### `pipeline_runs`

Stores pipeline execution metadata.

| Column          | Purpose                        |
| --------------- | ------------------------------ |
| `run_id`        | Unique pipeline run identifier |
| `pipeline_name` | Name of the pipeline           |
| `started_at`    | Pipeline start time            |
| `completed_at`  | Pipeline completion time       |
| `status`        | Recorded execution status      |
| `sla_deadline`  | Expected completion deadline   |
| `error_message` | Optional failure details       |

### `pipeline_metrics`

Stores data freshness and latency monitoring measurements.

| Column                  | Purpose                           |
| ----------------------- | --------------------------------- |
| `id`                    | Unique metric identifier          |
| `dataset_name`          | Dataset being monitored           |
| `latest_data_timestamp` | Latest event timestamp observed   |
| `checked_at`            | Time the freshness check occurred |
| `freshness_minutes`     | Age of the latest data in minutes |
| `latency_seconds`       | Measured latency in seconds       |
| `status`                | Recorded monitoring status        |

## Prerequisites

Install or configure the following:

* Python 3.10 or later
* Docker Desktop
* Git
* Visual Studio Code (recommended)

The project uses PostgreSQL through Docker Compose.

## Installation and Setup

### Step 1: Clone the Repository

```bash
git clone https://github.com/29akshay09/data-freshness-sla-monitoring.git
```

Navigate into the project directory:

```bash
cd data-freshness-sla-monitoring
```

### Step 2: Create a Virtual Environment

On Windows PowerShell:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

### Step 3: Install Dependencies

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If `requirements.txt` does not yet include the required packages, install them with:

```powershell
python -m pip install "psycopg[binary]" python-dotenv
```

Add any additional dependencies used by your implemented components to `requirements.txt`.

### Step 4: Configure Environment Variables

Create a local `.env` file by copying `.env.example`:

```powershell
Copy-Item .env.example .env
```

Open `.env` and set your local PostgreSQL password:

```dotenv
POSTGRES_PASSWORD=your_local_postgres_password
```

Replace the example value with the password configured for your PostgreSQL container.


### Step 5: Start PostgreSQL

Start the database container from the project root:

```powershell
docker compose up -d
```

Check its status:

```powershell
docker compose ps
```

The current local setup uses:

| Setting        | Value          |
| -------------- | -------------- |
| Database       | `freshness_db` |
| Username       | `akshay`       |
| Host           | `localhost`    |
| Host port      | `5433`         |
| Container port | `5432`         |

These values must match your local Docker Compose and Python configuration.

### Step 6: Initialize the Database

The SQL initialization script is located at:

```text
sql/create_tables.sql
```

It defines the database tables required by the project.

For an existing PostgreSQL volume, Docker's automatic initialization scripts do not run again simply because the SQL file changes. If your tables already exist, you do not need to recreate them. Otherwise, execute the SQL script against `freshness_db` using your preferred PostgreSQL client.

## Running the Components

Run these commands from the project root with the virtual environment activated.

### 1. Generate Sample Transactions

```powershell
python -m src.data_generator
```

### 2. Ingest Transactions into PostgreSQL

```powershell
python -m src.data_ingestion
```

### 3. Check Data Freshness

```powershell
python -m src.freshness_checker
```

Example output:

```text
INFO dataset=transactions
latest=2026-10-23 07:10:58+00:00
freshness=10.54m
threshold=15.0m
status=HEALTHY
```

The timestamp and freshness value above are illustrative; actual results depend on the data and execution time.

### 4. Calculate Ingestion Latency

```powershell
python -m src.latency_calculator
```

The component reports the number of transactions and their average, maximum, and minimum event-to-ingestion latency.

### 5. Check Pipeline SLA Status

```powershell
python -m src.sla_checker
```

The SLA checker reads the most recent pipeline-run records. If the `pipeline_runs` table is empty, it reports that no pipeline runs were found.

## Example Use Case

Imagine an analytics team expects its transaction dataset to be updated every 15 minutes.

If the latest event is older than the configured threshold, the freshness checker can identify the dataset as stale.

The latency calculator can then help quantify delays between event creation and database ingestion.

Separately, the SLA checker can compare a pipeline's completion time with its deadline.

Together, these checks provide a foundation for identifying data delivery problems before they affect downstream reporting.

## Future Improvements

The following are planned enhancements and should be marked complete only after implementation and testing.

* [ ] Add automated alerts for stale datasets and SLA breaches.
* [ ] Integrate monitoring jobs with Apache Airflow.
* [ ] Persist freshness and latency results into `pipeline_metrics`.
* [ ] Add scheduled pipeline health checks.
* [ ] Build Grafana dashboards for freshness, latency, and SLA compliance.
* [ ] Add structured logging and error handling.
* [ ] Create unit tests for freshness and SLA logic.
* [ ] Add automated tests for ingestion and database operations.
* [ ] Support configurable thresholds for multiple datasets.
* [ ] Add Docker-based instructions for running the complete application.

## Skills Demonstrated

This project demonstrates practical experience with:

* Python-based data engineering
* SQL and PostgreSQL
* Data ingestion and database connectivity
* Timestamp-based data freshness checks
* Event-to-ingestion latency calculations
* Pipeline SLA monitoring
* Docker-based local development
* Git and GitHub version control
* Data pipeline reliability and observability concepts

## Author

**Akshay Kamble**

GitHub: [@29akshay09](https://github.com/29akshay09)

---

If you find this project useful, feel free to explore the repository and follow its development.
