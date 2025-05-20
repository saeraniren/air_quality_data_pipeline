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
import numpy as np
import pandas as pd
from pprint import pprint

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
        raise

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
        raise

def upload_to_bigquery(**context):
    ti = context['ti']
    records = ti.xcom_pull(task_ids='parse_api_response', key='parsed_items')

    # XCom 데이터가 없을 시
    if not records:
        print("No data found in XCom for upload.")
        return 

    try:
        df = pd.json_normalize(records)

        # '24:00'이 포함된 행이 하나라도 있는지 확인
        if df['dataTime'].str.contains('24:00', na=False).any():
            mask_24 = df['dataTime'].str.contains('24:00', na=False)

            # 날짜만 추출한 후 하루를 더하고, '00:00' 형식으로 다시 넣기
            df.loc[mask_24, 'dataTime'] = (
                pd.to_datetime(df.loc[mask_24, 'dataTime'].str[:10]) + pd.Timedelta(days=1)
            ).dt.strftime('%Y-%m-%d 00:00')

        # dateTime 형변환
        df['dataTime'] = pd.to_datetime(df['dataTime'])
        df['dataTime'] = df['dataTime'].dt.strftime('%Y-%m-%d %H:%M')

        # float 처리
        float_columns = ['so2Value', 'o3Value', 'no2Value',
                         'pm10Value', 'pm10Value24', 'pm25Value', 'pm25Value24']

        # int 처리
        int_columns = ['khaiValue', 'khaiGrade', 'so2Grade', 'coGrade',
                       'o3Grade', 'no2Grade', 'pm10Grade', 'pm25Grade',
                       'pm10Grade1h', 'pm25Grade1h']

        # 형변환
        for col in df.columns:
            if col in float_columns and col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].replace({np.nan: None})

            elif col in int_columns and col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].astype('Int64').replace({pd.NA: None})

                # Int64 타입을 일반 int로 변환 (JSON 직렬화를 위해)
                df[col] = df[col].astype(object).where(df[col].notna(), None)

        # 이상값 처리
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None, '-': None})
        df = df.where(pd.notnull(df), None)

        if df.empty:
            print("DataFrame is empty. Nothing to load to BigQuery")
            raise

        # 인증정보 데이터
        key_path = Variable.get('GCP_ACCOUNT_FILE_PATH')
        credentials = service_account.Credentials.from_service_account_file(key_path)

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

        # JSON으로 안전하게 변환 - 특별히 특수 값 처리 추가
        # DataFrame을 dict 리스트로 변환하고 NaN 값 처리
        records_list = []

        for _, row in df.iterrows():
            row_dict = {}
            for col, val in row.items():
                # np.nan, pd.NA 등을 None으로 변환
                if pd.isna(val) or val is pd.NA:
                    row_dict[col] = None
                else:
                    row_dict[col] = val
            records_list.append(row_dict)

        # JSON object list direvtly passed
        job = client.load_table_from_json(
            records_list,
            destination=table_ref,
            job_config=job_config
            )
        
        job.result()

        print(f"Loaded {len(df)} rows to {table_ref}")

    except Exception as e:
        print(f"Failed to load JSON into DataFrame: {e}")
        raise

with DAG(
    dag_id='data_extract_transform_load',
    start_date=pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="55 * * * *",
    tags=['Extract', 'Transform', 'Load'],
    default_args=default_args,
    catchup=False
) as dag:

    api_task = HttpOperator(
        task_id='api_authentication',
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
