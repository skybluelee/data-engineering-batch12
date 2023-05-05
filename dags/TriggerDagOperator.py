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

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def etl(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]
    # api_key = Variable.get("open_weather_api_key")
    api_key = "2d61882f7e74a4787667e1de1e8004b1"

    # https://openweathermap.org/api/one-call-api
    url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts"
    response = requests.get(url)
    data = json.loads(response.text)

    """
    {'dt': 1622948400, 'sunrise': 1622923873, 'sunset': 1622976631, 'moonrise': 1622915520, 'moonset': 1622962620, 'moon_phase': 0.87, 'temp': {'day': 26.59, 'min': 15.67, 'max': 28.11, 'night': 22.68, 'eve': 26.29, 'morn': 15.67}, 'feels_like': {'day': 26.59, 'night': 22.2, 'eve': 26.29, 'morn': 15.36}, 'pressure': 1003, 'humidity': 30, 'dew_point': 7.56, 'wind_speed': 4.05, 'wind_deg': 250, 'wind_gust': 9.2, 'weather': [{'id': 802, 'main': 'Clouds', 'description': 'scattered clouds', 'icon': '03d'}], 'clouds': 44, 'pop': 0, 'uvi': 3}
    """
    ret = []
    for d in data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        ret.append("('{}',{},{},{})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))

    cur = get_Redshift_connection()
    
    # 임시 테이블 생성
    create_sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};
    CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS);INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table};"""
    logging.info(create_sql)
    try:
        cur.execute(create_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 임시 테이블 데이터 입력
    insert_sql = f"INSERT INTO {schema}.temp_{table} VALUES " + ",".join(ret)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 기존 테이블 대체
    alter_sql = f"""DELETE FROM {schema}.{table};
      INSERT INTO {schema}.{table}
      SELECT date, temp, min_temp, max_temp FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
        FROM {schema}.temp_{table}
      )
      WHERE seq = 1;"""
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

dag = DAG(
    dag_id = 'TriggerDagRunOperator',
    start_date = datetime(2022,8,24), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 4 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

trigger_B = TriggerDagRunOperator(
    task_id="trigger_dag_run_test",
    trigger_dag_id="Gsheet_to_Redshift",
    # DAG B에 넘기고 싶은 정보. DAG B에서는 Jinja 템플릿(dag_run.conf["path"])으로 접근 가능.
    # DAG B PythonOperator(**context)에서라면 kwargs['dag_run'].conf.get('conf')
    conf={ 'path': '/opt/ml/conf' },
    # Jinja 템플릿을 통해 DAG A의 execution_date을 패스
    execution_date="{{ ds }}",
    reset_dag_run=True, # True일 경우 해당 날짜가 이미 실행되었더라는 다시 재실행
    wait_for_completion=True, # DAG B가 끝날 때까지 기다릴지 여부를 결정. 디폴트값은 False
    dag = dag
)
etl = PythonOperator(
    task_id = 'etl',
    python_callable = etl,
    # 서울의 위도/경도
    params = {
        "lat": 37.5665,
        "lon": 126.9780,
        "schema": "kusdk",
        "table": "TriggerDagOperator"
    },
    dag = dag
)

trigger_B >> etl