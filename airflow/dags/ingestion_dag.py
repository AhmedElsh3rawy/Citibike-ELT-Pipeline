from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import zipfile, os, calendar
import pandas as pd
from sqlalchemy import create_engine

default_args = {"owner": "ahmed", "retries": 5, "retry_delay": timedelta(minutes=1)}

dag = DAG(
    dag_id="ingestion_dag",
    default_args=default_args,
    description="A DAG for downloading and ingesting data",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)


DATA_DIR = "/opt/airflow/data"
TABLE_NAME = "raw_trips"
DB_URL = "postgresql://postgres:password@postgres:5432/citibike_db"
CHUNK_SIZE = 100000


def download_zipped_data(year: int, ti):
    ti.xcom_push(key="year", value=year)

    output = f"{DATA_DIR}/{year}-citibike-tripdata.zip"
    url = f"https://s3.amazonaws.com/tripdata/{year}-citibike-tripdata.zip"

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(output, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    print(f"Downloaded data for year {year} to {output}")
    return output


def unzip_data(ti):
    zip_path = ti.xcom_pull(task_ids="download_zipped_data", key="return_value")
    year = ti.xcom_pull(task_ids="download_zipped_data", key="year")

    extract_to = f"{DATA_DIR}/"
    os.makedirs(extract_to, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_to)

    print(f"Extracted data to {extract_to}")
    return extract_to


def ingest_data_to_db(ti):
    year = ti.xcom_pull(task_ids="download_zipped_data", key="year")
    months = [f"{i}_{calendar.month_name[i]}" for i in range(1, 13)]

    dtype = {
        "tripduration": "Int64",
        "start station id": "Int64",
        "start station name": "string",
        "start station latitude": "float64",
        "start station longitude": "float64",
        "end station id": "Int64",
        "end station name": "string",
        "end station latitude": "float64",
        "end station longitude": "float64",
        "bikeid": "Int64",
        "usertype": "string",
        "birth year": "Int64",
    }

    parse_dates = ["starttime", "stoptime"]

    engine = create_engine(DB_URL)

    for i, month in enumerate(months, start=1):
        month_str = f"{i:02d}" if i < 10 else str(i)
        file_path = f"{DATA_DIR}/{year}-citibike-tripdata/{month}/{year}{month_str}-citibike-tripdata_1.csv"
        df_iter = pd.read_csv(
            file_path,
            dtype=dtype,
            parse_dates=parse_dates,
            chunksize=CHUNK_SIZE,
            header=0,
        )

        for chunk in df_iter:
            if i == 1 and chunk.index.start == 0:
                chunk.columns = (
                    chunk.columns.str.strip()
                    .str.lower()
                    .str.replace(" ", "_")
                    .str.replace("-", "_")
                )

                chunk.head(0).to_sql(
                    name=TABLE_NAME, con=engine, if_exists="replace", index=False
                )

            chunk.to_sql(name=TABLE_NAME, con=engine, if_exists="append", index=False)


with dag:
    task1 = PythonOperator(
        task_id="download_zipped_data",
        python_callable=download_zipped_data,
        op_kwargs={"year": 2014},
    )

    task2 = PythonOperator(task_id="unzip_data", python_callable=unzip_data)

    task3 = PythonOperator(
        task_id="ingest_data_to_db", python_callable=ingest_data_to_db
    )

    task1 >> task2 >> task3
