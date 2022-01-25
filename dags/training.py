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
from sktime.forecasting.arima import AutoARIMA
from sktime.forecasting.model_selection import temporal_train_test_split
from sktime.forecasting.base import ForecastingHorizon
from sktime.performance_metrics.forecasting import mean_absolute_percentage_error


DATABASE_URL = Variable.get("database_url")
SOURCE_TABLE = 'covid_19_geographic_distribution_worldwide'
DESTINATION_TABLE = 'models'
GEO_ID = 'CZ'
FIRST_DATE = date(2020, 9, 1)
NUMBER_OF_PERIODS = 10


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
        .query(tbl.c.date, tbl.c.daily_confirmed_cases)\
        .filter(tbl.c.geo_id==GEO_ID)\
        .filter(tbl.c.date>=FIRST_DATE)\
        .order_by(tbl.c.date)

    df = pd.read_sql(query.statement, query.session.bind)
    df.rename(columns={"date": "ds", "daily_confirmed_cases": "y"}, inplace=True)
    logging.info(f"Head: \n{df.head()}")
    logging.info(f"Tail: \n{df.tail()}")

    session.close()
    return df


def fit(data):
    forecaster = AutoARIMA(sp=NUMBER_OF_PERIODS)
    forecaster.fit(data)
    return forecaster


def evaluate(model, data):
    fh = ForecastingHorizon(data.index, is_relative=False)
    pred = model.predict(fh)
    logging.info(f"MAPE: {mean_absolute_percentage_error(data, pred)}")


def transform(data):
    y = data.set_index('ds').asfreq('D')['y']
    train, test = temporal_train_test_split(y)
    model = fit(train)
    evaluate(model, test)
    return model


def load(model):
    engine = sqlalchemy.create_engine(DATABASE_URL)
    metadata = sqlalchemy.MetaData(engine)
    table = sqlalchemy.Table(DESTINATION_TABLE, metadata,
              sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, nullable=False), 
              sqlalchemy.Column('created', sqlalchemy.DateTime, default=datetime.utcnow), 
              sqlalchemy.Column('model_name', sqlalchemy.String), 
              sqlalchemy.Column('geo_id', sqlalchemy.String),
              sqlalchemy.Column('model_file', sqlalchemy.LargeBinary),
        )

    insp = sqlalchemy.inspect(engine)
    if not insp.dialect.has_table(engine, DESTINATION_TABLE):
        table.create(engine)

    model_pickle = pickle.dumps(model)
    ins = table.insert().values(model_name='AutoARIMA', geo_id=GEO_ID, model_file=model_pickle)

    with engine.connect() as connection:
        connection.execute(ins)


def etl():
    data = extract()
    model = transform(data)
    load(model)


with DAG(
    dag_id="training",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    training = PythonOperator(task_id="training", python_callable=etl)

training
