from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator



def print_hello():
    print("Hello world!")



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 5, 5),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


dag = DAG(
    "hello_world",
    description = "Simple tutorial DAG",
    schedule_interval = "0 12 * * *",
    default_args = default_args,
    catchup = False
)


t1 = EmptyOperator(task_id="dummy_task", retries=3, dag=dag)
t2 = PythonOperator(task_id="hello_task", python_callable=print_hello, dag=dag)


# Establish dependencies
t1 >> t2

# Equivalent
#t2.set_upstream(t1)

# or
# t1.set_downstream(t2)
