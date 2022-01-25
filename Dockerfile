FROM apache/airflow:2.2.3
RUN pip install --upgrade pip
RUN pip install --no-cache-dir sktime==0.9.0 pmdarima==1.8.4
