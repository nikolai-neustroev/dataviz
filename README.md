# dataviz
Data visualization system

Dataviz exports data from BigQuery, forecast time series, and visualize them in Grafana.

## Requirements
- Google Cloud service account JSON key file 
- Python 3.7
- Docker Compose

## Installation
1. Run `init.sh` file. It will create Python virtual environment and install Airflow.
`source init.sh`
2. Put JSON key file to `secrets` directory.
3. Run `docker-compose up`

## Using Dataviz
Open Airflow UI at `0.0.0.0:8080`. Сonsequently run "extract", "training" and "inference" dags. 
"extract" imports data from BigQuery to local Postres database.
"training" training trains AutoARIMA forecaster for "daily_confirmed_cases" time series.
"inference" creates forecast for configured horizon.

Further, open Grafana at `0.0.0.0:3000`. It will visualize actual and forecasted data for "daily_confirmed_cases".
