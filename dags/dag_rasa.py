from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'Eric Silveira',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1
}


@dag(
    dag_id='rasa_data_ingestion_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
)
def rasa_data_ingestion_dag():
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    

    start >> end

instancia_dag_ingestion = rasa_data_ingestion_dag()