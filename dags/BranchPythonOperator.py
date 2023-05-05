from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json

from airflow.operators.python import BranchPythonOperator
# 상황에 따라 뒤에 실행되어야 하는 태스크를 리턴
def skip_or_cont_trigger():
    if Variable.get("mode", "dev") == "dev":
        return ["print_hello"]
    else:
        return ["print_goodbye"]
# "mode"라는 Variable의 값이 "dev"이면 trigger_b 태스크를 스킵

dag = DAG(
    dag_id = 'BranchPythonOperator',
    start_date = datetime(2022,8,24), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 4 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=skip_or_cont_trigger,
    dag = dag
)

def print_hello():
    print("hello!")
    return "hello!"

def print_goodbye():
    print("goodbye!")
    return "goodbye!"

print_hello = PythonOperator(
    task_id = 'print_hello',
    #python_callable param points to the function you want to run 
    python_callable = print_hello,
    #dag param points to the DAG that this task is a part of
    dag = dag
)

print_goodbye = PythonOperator(
    task_id = 'print_goodbye',
    python_callable = print_goodbye,
    dag = dag
)

branching >> [print_hello, print_goodbye]