from datetime import date, datetime
import logging
import pickle

from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sktime.forecasting.base import ForecastingHorizon


DATABASE_URL = Variable.get("database_url")
SOURCE_TABLE = 'models'
MODEL_NAME = 'AutoARIMA'
GEO_ID = 'CZ'
HORIZON = 17
START_DATE = date(2020, 12, 15)


def extract():
    DATABASE_URL = Variable.get("database_url")
    engine = sqlalchemy.create_engine(DATABASE_URL)
    session_factory = sessionmaker(bind=engine)
    session = scoped_session(session_factory)
    
    metadata = sqlalchemy.MetaData()
    tbl = sqlalchemy.Table(
        SOURCE_TABLE, 
        metadata, 
        autoload=True, 
        autoload_with=engine
    )
    query = session\
        .query(tbl.c.model_file)\
        .filter(tbl.c.geo_id==GEO_ID)\
        .filter(tbl.c.model_name==MODEL_NAME)\
        .order_by(tbl.c.id.desc())\
        .limit(1)

    with engine.connect() as connection:
        result = connection.execute(query.statement)
        
    model = result.first()['model_file']
    return model


def transform(model):
    date_range = pd.DatetimeIndex(pd.date_range(START_DATE, periods=HORIZON, freq='D'))
    logging.info(date_range)
    fh = ForecastingHorizon(date_range, is_relative=False)
    forecaster = pickle.loads(model)
    forecast = forecaster.predict(fh)
    forecast = forecast.to_frame()
    forecast = forecast.reset_index()
    forecast = forecast.rename(columns={"index": "date", 0: "daily_confirmed_cases"})
    logging.info(f"Type: {type(forecast)}")
    logging.info(forecast)
    return forecast


def load(forecast):
    engine = sqlalchemy.create_engine(DATABASE_URL)
    with engine.connect() as connection:
        forecast.to_sql('forecast', connection, if_exists='append')


def etl():
    model = extract()
    forecast = transform(model)
    load(forecast)


with DAG(
    dag_id="inference",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    inference = PythonOperator(task_id="inference", python_callable=etl)

inference
