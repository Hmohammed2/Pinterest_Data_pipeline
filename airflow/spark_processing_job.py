from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable

work_dir = Variable.get("pinterest_dir")

default_args = {
    'owner': 'Hamza',
    'depends_on_past': False,
    'retries': 1,
    'start_date': datetime(2023, 7, 25),
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='batch_processing',
         default_args=default_args,
         schedule='0 10 * * *',
         catchup=False,
         ) as dag:

    batch_consume_task = BashOperator(
        task_id='consume_batch_data',
        bash_command=f'cd {work_dir} && python batch_consumer.py '
    )
    batch_process_task = BashOperator(
        task_id='process_batch_data',
        bash_command=f'cd {work_dir} && python batch_processing.py'
    )

    
    batch_consume_task >> batch_process_task