from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# pfad zum Hauptverzeichnis hinzufügen, damit ariflow die scripte findet
from job_scout import start_scout

# 1.Standard Einstellungen Dag
default_args = {
    'owner': 'it_market_radar',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), #ab wnann dag laufen kann
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,                       # Bei Fehler einmal neu versuchen
    'retry_delay': timedelta(minutes=5), # 5 Minuten warten vor Neustart
}

#Zeitplan des Dags
with DAG(
    'it_market_radar_ingestion',        # Name des DAGs in der Airflow-Oberfläche
    default_args=default_args,
    description='Holt Jobdaten von Adzuna und sendet sie an Kafka',
    schedule_interval=timedelta(hours=1), # stündliche Ausführung
    catchup=False                       #
) as dag:

    #Task
    fetch_and_send_to_kafka = PythonOperator(
        task_id='fetch_jobs_from_adzuna',
        python_callable=start_scout     # Funktionsaufruf
    )

    # nur eine Task
    fetch_and_send_to_kafka
