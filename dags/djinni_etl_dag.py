from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

from functions.extract import extract_djinni
from functions.transform import clean_text
from functions.load import upload_to_s3


INPUT_FILE_PATH = "input/categories.txt"
RAW_DATA_FILE_PATH = "data/raw"
PROCESSED_DATA_FILE_PATH = "data/processed"
current_date = datetime.today().strftime("%Y-%m-%d")


with DAG(dag_id="djinni_etl",
         start_date=datetime(2024, 2, 5),
         schedule_interval="@hourly",
         catchup=False) as dag:

    extract = PythonOperator(
        task_id="extract_vacancies_data",
        python_callable=extract_djinni,
        op_kwargs={
            "INPUT_FILE_PATH": INPUT_FILE_PATH,
            "RAW_DATA_FILE_PATH": RAW_DATA_FILE_PATH,
            "current_date": current_date
        })

    clean = PythonOperator(
        task_id="clean_full_text",
        python_callable=clean_text,
        op_kwargs={
            "RAW_DATA_FILE_PATH": RAW_DATA_FILE_PATH,
            "PROCESSED_DATA_FILE_PATH": PROCESSED_DATA_FILE_PATH,
            "current_date": current_date
        })

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "PROCESSED_DATA_FILE_PATH": PROCESSED_DATA_FILE_PATH,
            "current_date": current_date,
            "bucket_name": "djinni-processed-bucket"
        })

    extract >> clean >> upload
