import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator  
from airflow.operators.python import PythonOperator
#from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.hooks.base import BaseHook
import json
import sqlite3
import logging

def health_check_response(response):
    logging.info(f"Response status code: {response.status_code}")
    logging.info(f"Response body: {response.text}")
    return response.status_code == 200 and response.json() == {"message": "API health check successful"}

def insert_update_player_data(**context):
# Fetch the connection object
    conn_id = 'sqlite_default'
    connection = BaseHook.get_connection(conn_id)
    
    # Extract the path to the SQLite database from the connection object
    sqlite_db_path = connection.host
    
    player_data = context['ti'].xcom_pull(task_ids='api_player_query')
    
    if player_data:
        player_data = json.loads(player_data)  # Convert from JSON string to Python dictionary/list
        
        # Use a context manager for the SQLite connection
        with sqlite3.connect(sqlite_db_path) as conn:
            cursor = conn.cursor()

            # Insert each player record into the 'player' table
            for player in player_data:
                try:
                    cursor.execute("""
                    INSERT INTO player (player_id, gsis_id, first_name, last_name, position, last_changed_date) 
                    VALUES (?, ?, ?, ?, ?, ?) 
                    ON CONFLICT(player_id) DO UPDATE SET 
                    gsis_id=excluded.gsis_id, first_name=excluded.first_name, last_name=excluded.last_name, 
                    position=excluded.position, last_changed_date=excluded.last_changed_date
                    """, (
                        player['player_id'], player['gsis_id'], player['first_name'], 
                        player['last_name'], player['position'], player['last_changed_date']
                    ))
                except Exception as e:
                    logging.error(f"Failed to insert player {player['player_id']}: {e}")
                    

    else:
        logging.warning("No player data found.")


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
    #    endpoint='/v0/players/?skip=0&limit=100000&minimum_last_changed_date=2024-04-01', 
        endpoint='/v0/players/?skip=0&limit=100000&minimum_last_changed_date={{ (data_interval_start - macros.timedelta(days=1)) | ds }}',
        method='GET',
        headers={"Content-Type": "application/json"},
    )

    # # Task to transform and upsert data into SQLite
    # player_upsert_task = PythonOperator(
    #     task_id='player_upsert',
    #     python_callable=insert_update_player_data,
    #     provide_context=True,
    # )

    player_sqlite_upsert_task = PythonOperator(
        task_id='player_sqlite_upsert',
        python_callable=insert_update_player_data,
        provide_context=True,
    )


    # Define the task dependencies
    api_health_check_task >> api_player_query_task >> player_sqlite_upsert_task

# Instantiate the DAG
dag_instance = recurring_player_api_insert_update_dag()
