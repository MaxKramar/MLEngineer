# dags/alt_churn.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.churn import create_table, extract, transform, load # импортируем фукнции с логикой шагов
from steps.messages import send_telegram_success_message, send_telegram_failure_message
import pendulum

# инициализируем задачи DAG, указывая параметр python_callable

with DAG(
    dag_id='churn',
    schedule='@once',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message) as dag:
    
    create_step = PythonOperator(task_id='create', python_callable=create_table)
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    load_step = PythonOperator(task_id='load', python_callable=load)

    create_step >> extract_step >> transform_step >> load_step