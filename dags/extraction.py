# Export table data from BigQuery to CSV
from datetime import datetime
import logging

from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import sqlalchemy


SERVICE_ACCOUNT_JSON = Variable.get("google_application_credentials")
DATABASE_URL = Variable.get("database_url")
PROJECT =  'bigquery-public-data'
DATASET_ID = 'covid19_ecdc_eu'
TABLE_ID = 'covid_19_geographic_distribution_worldwide'
FIELDS = [
    'date', 
    'daily_confirmed_cases', 
    'daily_deaths', 
    'confirmed_cases', 
    'deaths', 
    'countries_and_territories', 
    'geo_id', 
    'country_territory_code', 
    'pop_data_2019',
]


def extract():
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
    QUERY = f"""
    SELECT {', '.join(FIELDS)} 
    FROM `{PROJECT}.{DATASET_ID}.{TABLE_ID}`
    """
    df = client.query(QUERY).to_dataframe()

    engine = sqlalchemy.create_engine(DATABASE_URL)
    with engine.connect() as connection:
        df.to_sql(TABLE_ID, connection, if_exists='replace')
    logging.info(f'Table {TABLE_ID} successfully stored in database.')


with DAG(
    dag_id="extract",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    extraction = PythonOperator(task_id="extract", python_callable=extract)

extraction
