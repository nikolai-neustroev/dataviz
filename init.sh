python3.7 -m venv venv
source venv/bin/activate

mkdir -p ./dags ./logs ./plugins ./secrets ./data
echo -e "AIRFLOW_UID=$(id -u)" > .env

AIRFLOW_VERSION=2.2.3

PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-no-providers-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

docker build -t dataviz .
export AIRFLOW_IMAGE_NAME=dataviz
