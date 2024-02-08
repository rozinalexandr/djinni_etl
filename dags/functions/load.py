from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_to_s3(PROCESSED_DATA_FILE_PATH: str, current_date: str, bucket_name: str):
    path_to_file = f"{PROCESSED_DATA_FILE_PATH}/{current_date}_processed.csv"

    hook = S3Hook("s3_conn")
    hook.load_file(filename=path_to_file, key=f"{current_date}_processed.csv", bucket_name=bucket_name)
