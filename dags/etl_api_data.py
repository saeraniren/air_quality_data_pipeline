import pendulum
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.http.operators.http import HttpOperator

import json
import pandas as pd
from io import BytesIO
from pytz import timezone

default_args = {
    'owner': 'saeran.iren',
    'email': ['saeran.iren@airflow.com'],
    'email_on_failure': False,
    'retries': 3
}

def parse_response(**context):
    ti = context['ti']
    task_id = 'api_authentication'
    response_text = ti.xcom_pull(task_ids=task_id)

    if not response_text:
        print("No response received from XCom.")
        return

    try:
        response_json = json.loads(response_text)
        items = response_json.get('response', {}).get('body', {}).get('items', [])

        if items:
            ti.xcom_push(key='parsed_items', value=items)
            print(f'Pushed items to XCom.')
        else:
            print("No items forund in API response.")

    except json.JSONDecodeError:
        print(" JSON decoding failed.")
        return

def upload_to_bigquery(**context):
    ti = context['ti']
    records = ti.xcom_pull(task_ids='parse_api_response', key='parsed_items')

    # 인증정보 데이터
    key_path = Variable.get('GCP_ACCOUNT_FILE_PATH')
    credentials = service_account.Credentials.from_service_account_file(key_path)

    # XCom 데이터가 없을 시
    if not records:
        print("No data found in XCom for upload.")
        return 

    try:
        df = pd.json_normalize(records)

        # 문자열로 들어온 숫자 필드를 명시적으로 float으로 변환
        # 숫자 필드에서 '-' 같은 문자열을 None으로 바꾸기 (to_numeric 전에)
        float_columns = ['o3Value', 'pm10Value', 'pm25Value', 'coValue', 'no2Value', 'so2Value']
        for col in float_columns:
            if col in df.columns:
                df[col] = df[col].replace({'-': None})
        
        # 그 다음에 숫자 변환
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 이상값 처리
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, '-': None})
        df = df.where(pd.notnull(df), None)

        if df.empty:
            print("DataFrame is empty. Nothing to load to BigQuery")
            return

        # Table Info
        project_id = 'vibrant-map-459723-k4'
        dataset_id = 'sprint_mission_18'
        table_id = 'air_quality_from_seoul'
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        # BigQuery client
        client = bigquery.Client(credentials=credentials, project=project_id)

        dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
        
        # 데이터셋 존재여부 확인
        try:
            client.get_dataset(dataset_ref)
            print(f"Dataset {dataset_id} already exists.")
            
        except NotFound:
            print(f'Dataset {dataset_id} not found. Creating it...')
            client.create_dataset(dataset_ref)
            print(f"Dataset {dataset_id} created")

        # 업로드
        job_config = bigquery.LoadJobConfig(
            write_disposition='WRITE_APPEND',
            autodetect = True,
        )

        # JSON object list direvtly passed
        job = client.load_table_from_json(
            df.to_dict(orient='records'),
            destination=table_ref,
            job_config=job_config
            )
        
        job.result()

        print(f"Loaded {len(df)} rows to {table_ref}")

    except Exception as e:
        print(f"Failed to load JSON into DataFrame: {e}")
        return

with DAG(
    dag_id='data_extract_transform_load',
    start_date=pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="20 * * * *",
    tags=['Extract'],
    default_args=default_args,
    catchup=False
) as dag:

    api_task = HttpOperator(
        task_id=f'api_authentication',
        method='GET',
        http_conn_id='datagokr_apikey',
        endpoint='/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty',
        data={
            'serviceKey': Variable.get('DATAGOKR_API_KEY'),
            'returnType': 'json',
            'numOfRows': '500',
            'pageNo': '1',
            'sidoName': '서울',
        },
        headers={},
        log_response=True
    )

    parse_data = PythonOperator(
        task_id='parse_api_response',
        python_callable=parse_response,
    )

    transform_and_load_task = PythonOperator(
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery,
    )

    api_task >> parse_data >> transform_and_load_task
