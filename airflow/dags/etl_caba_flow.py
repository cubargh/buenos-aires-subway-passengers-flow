import os
from zipfile import ZipFile

import pandas as pd
import requests as r
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def extract_and_combine_data(**kwargs) -> pd.DataFrame:

    url = "https://cdn.buenosaires.gob.ar/datosabiertos/datasets/sbase/subte-viajes-molinetes/molinetes-2023.zip"

    # download file to tmp location
    response = r.get(url)
    with open("./tmp/molinetes.zip", "wb") as file:
        file.write(response.content)
    zip_file = ZipFile("./tmp/molinetes.zip")

    # extract and combine data
    dfs = []
    files = zip_file.namelist()
    for file in files:
        if file.endswith(".csv"):
            with zip_file.open(file) as f:
                pd.read_csv(f, sep=";").to_csv(
                    "./tmp/tmp_molinetes.csv"
                )  # cant understand why it doesn't parse it on the first time, so I have to save it to a tmp file and then read it again...
                csv = pd.read_csv("./tmp/tmp_molinetes.csv", sep=";")
                dfs.append(csv)

    # return combined data
    concat_df = pd.concat(dfs).iloc[:, 0:10]
    concat_df.to_parquet("./tmp/combined_molinetes.parquet")
    return concat_df


def transform_data(**kwargs):

    # Load the data
    df = pd.read_parquet("./tmp/combined_molinetes.parquet")

    # Transform the data
    df.columns = df.columns.str.replace(",", "").str.lower().str.replace(" ", "_")
    df["fecha"] = df["fecha"].str.extract(r",(.*)", expand=False)  # fix leading comma

    # Return the transformed data
    df.to_parquet("./tmp/transformed_molinetes.parquet")


def send_to_bucket(**kwargs):

    import pyarrow as pa
    import pyarrow.parquet as pq

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./tmp/key.json"

    # Load the data
    data = pd.read_parquet("./tmp/transformed_molinetes.parquet")
    data["mes"] = pd.to_datetime(data["fecha"], format="%d/%m/%Y").dt.month

    # Write the data to bucket
    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()
    pq.write_to_dataset(
        table, root_path="caba_flow/data", filesystem=gcs, partition_cols=["mes"], compression="snappy"
    )


# Define the DAG
with DAG(
    "caba_etl",
    default_args=default_args,
    description="DAG to download data from the Buenos Aires government portal and push it to GS Bucket.",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["caba"],
) as dag:

    # Task to extract and combine data
    extract_and_combine_task = PythonOperator(
        task_id="extract_and_combine_data",
        python_callable=extract_and_combine_data,
        provide_context=True,
    )
    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )
    send_to_bucket_task = PythonOperator(
        task_id="send_to_bucket",
        python_callable=send_to_bucket,
        provide_context=True,
    )

    # Set task dependencies
    extract_and_combine_task >> transform_data_task >> send_to_bucket_task
