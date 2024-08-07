import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator  # Change the import from SimpleHttpOperator to HttpOperator
import logging

def health_check_response(response):
    logging.info(f"Response status code: {response.status_code}")
    logging.info(f"Response body: {response.text}")
    return response.status_code == 200 and response.json() == {"message": "API health check successful"}

@dag(start_date=datetime.datetime(2021, 1, 1), schedule_interval="@daily")
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

    # Define the task dependencies
    api_health_check_task >> api_player_query_task

# Instantiate the DAG
dag_instance = recurring_player_api_insert_update_dag()
