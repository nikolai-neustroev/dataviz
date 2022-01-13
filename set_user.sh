mkdir -p ./dags ./logs ./plugins ./secrets ./data
echo -e "AIRFLOW_UID=$(id -u)" > .env
