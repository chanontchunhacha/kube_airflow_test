from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task_group
from airflow.utils.dates import days_ago

import time

default_args ={
    'owner':'aom'
    #'start_date':pendulum.datetime(2023, 7, 4, 11, 30, 0, tz="Asia/Bangkok")
}

def test_function():
  result = 0
  for i in range(1,101):
    result += i**2
  if result % 2 == 0:
    output = 'Even'
  else:
    output = 'Odd'
  time.sleep(20)
  print("Output", output)
    
def hello_massage(massage):
    print(f'hello'+ massage)

with DAG(
  dag_id ='30task_test',
  default_args=default_args,
  schedule_interval=None,
  start_date=days_ago(1)
  ) as dag:

  @task_group()
  def group1():
    for i in range(10):
      PythonOperator(
        task_id ="test"+str(i+1),
        python_callable = test_function
      )
  
  t2 = PythonOperator(
    task_id = 'end',
    python_callable = hello_massage,
    op_kwargs={"massage":"END"}
  )
  
  group1() >> t2