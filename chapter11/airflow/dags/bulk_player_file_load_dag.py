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
import pandas as pd
import httpx



def download_parquet_file(url, local_file_path):
        with httpx.Client() as client:
            response = client.get(url)
            response.raise_for_status()  
            with open(local_file_path, 'wb') as file:
                file.write(response.content)

def retrieve_bulk_player_file(**context):
    parquet_file_url = "https://raw.githubusercontent.com/handsonapibook/apibook-part-one/main/bulk/player_data.parquet"
    local_file_path = "bulk_player_file.parquet"

    download_parquet_file(parquet_file_url, local_file_path)

    context['ti'].xcom_push(key='local_parquet_file_path', value=local_file_path)

def insert_update_player_data_bulk(**context):
# Fetch the connection object
    conn_id = 'sqlite_default'
    connection = BaseHook.get_connection(conn_id)
    
    sqlite_db_path = connection.host

    local_parquet_file_path = context['ti'].xcom_pull(task_ids='bulk_file_retrieve', key='local_parquet_file_path')

    
    if local_parquet_file_path:

        player_df = pd.read_parquet(local_parquet_file_path)
        
        # Use a context manager for the SQLite connection
        with sqlite3.connect(sqlite_db_path) as conn:
            cursor = conn.cursor()

            # Insert each player record into the 'player' table
            for _, player in player_df.iterrows():
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
def bulk_player_file_load_dag():


    bulk_file_retrieve_task = PythonOperator(
        task_id='bulk_file_retrieve',
        python_callable=retrieve_bulk_player_file,
        provide_context=True,
    )


    player_sqlite_upsert_task = PythonOperator(
        task_id='player_sqlite_upsert',
        python_callable=insert_update_player_data_bulk,
        provide_context=True,
    )


    # Define the task dependencies
    bulk_file_retrieve_task >> player_sqlite_upsert_task

# Instantiate the DAG
dag_instance = bulk_player_file_load_dag()
