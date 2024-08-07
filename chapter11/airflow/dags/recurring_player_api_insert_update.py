import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator  
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
import json
import sqlite3
import logging

def health_check_response(response):
    logging.info(f"Response status code: {response.status_code}")
    logging.info(f"Response body: {response.text}")
    return response.status_code == 200 and response.json() == {"message": "API health check successful"}

def insert_update_player_data(**context):
    # Extract the data from XCom
    player_data = context['task_instance'].xcom_pull(task_ids='api_player_query')
    player_dictionary = json.loads(player_data)  
    logging.info(f"Player data from xcom: {player_dictionary}")
    #Use the SQLite operator
    #pick up from here, use the database local_data.db and "player" table.


@dag(start_date=datetime.datetime(2024, 8, 7), schedule_interval=None, catchup=False)  
def recurring_player_api_insert_update_dag():
    api_health_check_task = HttpOperator( 
        task_id='check_api_health_check_endpoint',
        http_conn_id='http_default',  
        endpoint='/', 
        method='GET',
        headers={"Content-Type": "application/json"},
        response_check=health_check_response,
    )

    api_player_query_task = HttpOperator(  
        task_id='api_player_query',
        http_conn_id='http_default',  
        endpoint='/v0/players/?skip=0&limit=100000&minimum_last_changed_date=2024-04-01', 
        method='GET',
        headers={"Content-Type": "application/json"},
    )

    # Task to transform and upsert data into SQLite
    player_upsert_task = PythonOperator(
        task_id='player_upsert',
        python_callable=insert_update_player_data,
        provide_context=True,
    )


    # Define the task dependencies
    api_health_check_task >> api_player_query_task >> player_upsert_task

# Instantiate the DAG
dag_instance = recurring_player_api_insert_update_dag()
