from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
import os
import requests



default_args = {
   'depends_on_past': False,
   'email': ['airflow@example.com'],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5)
}



with DAG (
   'Etl',
   default_args = default_args,
   description = 'A simple DAG tutorial using MSSQL',
   schedule_interval = '@once', 
   start_date = datetime(2022, 5, 11),
   catchup = False,
   tags = ['example_mssql']
) as dag:

   """The most basic ETL pipeline your soul will ever witness"""

   create_employees_table = MsSqlOperator(
      task_id="create_employees_table",
      mssql_conn_id="airflow_mssql",
      sql="""
      if object_id('dbo.employees') is not null
         drop table dbo.employees;

      create table dbo.employees (
         "Serial Number" INT PRIMARY KEY,
         "Company Name" VARCHAR(1000),
         "Employee Markme" VARCHAR(1000),
         "Description" VARCHAR(1000),
         "Leave" INT
      );
      """
   )


   create_employees_temp_table = MsSqlOperator(
      task_id="create_employees_temp_table",
      mssql_conn_id="airflow_mssql",
      sql="""
      if object_id('dbo.employees_temp') is not null
         drop table dbo.employees_temp;

      create table dbo.employees_temp (
         "Serial Number" INT PRIMARY KEY,
         "Company Name" VARCHAR(1000),
         "Employee Markme" VARCHAR(1000),
         "Description" VARCHAR(1000),
         "Leave" INT
      );
      """,
   )


   @dag.task(task_id='insert_mssql_task')
   def get_data():

      data_path = "./files/employees.csv"

      try:
         os.makedirs(os.path.dirname(data_path), exist_ok=False)
      except Exception as e:
         pass


      url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/pipeline_example.csv"
      response = requests.get(url)


      with open(data_path, "w") as file:
         file.write(response.text)



   # transfer data to docker 
   transfer_data = BashOperator(
      task_id='transfer_data',
      bash_command="/Users/rashid/airflow/dags/scripts/test.sh "
   )

   
      
   @dag.task(task_id="bulk_insert_data")
   def insert_data():
      try:
         mssql_hook = MsSqlHook(mssql_conn_id='airflow_mssql', schema='jade') 
         bulk_sql = f"""BULK INSERT dbo.employees_temp FROM '/var/opt/mssql/employees.csv' WITH (FIRSTROW=2, FORMAT='CSV')"""
         mssql_hook.run(bulk_sql)
      except Exception as e:
         print(e)
         print("Did not go as planned")
         return 1
      else:
         print("Successfully saved data")
         return 0
     


   @task
   def merge_data():
      query = r"""
         TRUNCATE TABLE dbo.employees;

         MERGE
         INTO dbo.employees as target
         USING (
            SELECT DISTINCT * FROM dbo.employees_temp
         ) as source
         ON (target."Serial Number" = source."Serial Number")
         WHEN MATCHED
            THEN UPDATE
               SET target."Serial Number" = source."Serial Number"
         WHEN NOT MATCHED
            THEN INSERT VALUES (source."Serial Number", source."Company Name", source."Employee Markme", source."Description", source."Leave");
      """

      try:
         mssql_hook = MsSqlHook(mssql_conn_id="airflow_mssql", schema="jade")
         mssql_hook.run(query)

      except Exception as e:
         return 1

      else:
         print("Successfully merged employees")
         return 0


 
   # Completing our DAG
   [create_employees_table, create_employees_temp_table] >> get_data() >> transfer_data >> insert_data() >> merge_data()

 
