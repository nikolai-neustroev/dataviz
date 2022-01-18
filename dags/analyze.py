from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker


def analyze():
    DATABASE_URL = Variable.get("database_url")
    engine = sqlalchemy.create_engine(DATABASE_URL)
    session_factory = sessionmaker(bind=engine)
    session = scoped_session(session_factory)
    
    metadata = sqlalchemy.MetaData()
    tbl = sqlalchemy.Table(
        'covid_19_geographic_distribution_worldwide', 
        metadata, 
        autoload=True, 
        autoload_with=engine
    )
    query = session.query(tbl)

    df = pd.read_sql(query.statement, query.session.bind)
    print(df.shape)


with DAG(
    dag_id="analyze",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    analysis = PythonOperator(task_id="analyze", python_callable=analyze)

analysis
