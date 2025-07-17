from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Connection
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import snowflake.connector
import os

def fetch_ebay_data(**context):
    # Get search query and limit from Airflow Variables
    query = Variable.get("ebay_search_query", default_var="laptop")
    limit = Variable.get("ebay_result_limit", default_var="10")

    # Get eBay OAuth token from Airflow Variables
    ebay_token = Variable.get("ebay_oauth_token")

    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    headers = {
        "Authorization": f"Bearer {ebay_token}",
        "Content-Type": "application/json"
    }
    params = {
        "q": query,
        "limit": limit
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    items = response.json().get("itemSummaries", [])

    df = pd.json_normalize(items)
    csv_path = "/tmp/ebay_data.csv"
    df.to_csv(csv_path, index=False)

    context['ti'].xcom_push(key='csv_path', value=csv_path)

def load_to_snowflake(**context):
    # Get Snowflake connection
    conn: Connection = BaseHook.get_connection("snowflake_conn")
    csv_path = context['ti'].xcom_pull(task_ids='fetch_ebay_data', key='csv_path')

    sf_conn = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson.get("account"),
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.schema.split('.')[0],
        schema=conn.schema.split('.')[1]
    )

    cursor = sf_conn.cursor()

    cursor.execute("""
        CREATE OR REPLACE TABLE IF NOT EXISTS ebay_data (
            itemId STRING,
            title STRING,
            price_amount NUMBER,
            price_currency STRING,
            condition STRING,
            seller_username STRING,
            itemWebUrl STRING
        )
    """)

    # OPTIONAL: truncate table before loading
    # cursor.execute("TRUNCATE TABLE ebay_data")

    cursor.execute(f"""
        PUT file://{csv_path} @%ebay_data AUTO_COMPRESS=TRUE
    """)

    cursor.execute("""
        COPY INTO ebay_data
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        ON_ERROR = 'CONTINUE'
    """)

    cursor.close()
    sf_conn.close()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ebay_to_snowflake_daily',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ebay', 'snowflake']
) as dag:

    fetch = PythonOperator(
        task_id='fetch_ebay_data',
        python_callable=fetch_ebay_data,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    fetch >> load
