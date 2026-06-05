# Overview

Orchestration of a fully automated ETL pipeline for flight operations data using Apache Airflow. The pipeline features data ingestion from the OpenSky Network API, transformation of flight data, and loading of cleaned, business-ready data into Snowflake for analytics.

[See the OpenSky Network API docs](https://openskynetwork.github.io/opensky-api/rest.html)

## Project structure

- `dags/` - Airflow DAG definitions
- `scripts/` - ingestion, transformation, aggregation, and loading helper scripts
- `data/` - input/output datasets (generated / downloaded data)
- `docker-compose.yml` - local development environment with Airflow
- `requirements.txt` - application dependencies

## Prerequisites

- Docker and Docker Compose installed
- Python 3.11+ (for local dependency management and any script inspection)
- A `.env` file created from `.env.example`
- Access to the OpenSky Network API and Snowflake credentials for the target environment

## Getting started

1. Copy `.env.example` to `.env` and fill in your local connection and Airflow credentials:

```bash
cp .env.example .env
```

2. Start the stack:

```bash
docker compose up -d
```

3. Access the Airflow web UI, typically at `http://localhost:8080`.

## Notes

- The `data/` folder contains samples of generated datasets at every stage of the pipeline process.

## Pipeline Architecture

The project uses the medallion architecture to process OpenSky API data. This pattern organizes the lakehouse into layers that progressively improve data quality and structure as the data moves through the pipeline.

See the layers used in this project:

- `Bronze Layer` - raw API JSON ingestion from the OpenSky API. See the [gamma_ingest.py](./scripts/gamma_ingest.py) script for the bronze layer task.
- `Silver Layer` - cleaning and normalizing the flight data. See the [beta_transform.py](./scripts/beta_transform.py) script for the silver layer task.
- `Gold Layer` - aggregation of flight KPIs before loading data into Snowflake. See the [alpha_aggregate.py](./scripts/alpha_aggregate.py) script for the gold layer task.


### Pipeline Architecture Diagram

![Flight Operations Pipeline](/_images/flight-operations-pipeline.png "Flight Operations Pipeline Image!")


## What's Next?

- Expand source coverage with additional flight or aviation APIs
- Add DAG-level tests or use Airflow testing utilities
- Add logging/monitoring for failed tasks
- Add Snowflake schema validation and load verification




