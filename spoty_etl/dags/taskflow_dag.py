from distutils.command.clean import clean
import json
from datetime import timedelta
from airflow.decorators import dag,task
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
from base_etl import extract, transform, load
#from common_args import default_args
import pandas as pd
import pendulum

# [START instantiate_dag]
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)

def taskflow_api_etl():
    @task()
    def extract_task():
        context = get_current_context()
        return extract(context.execution_date)

#    @task(multiple_outputs=True)
    @task()
    def transform_task(json_df):
        context = get_current_context()
        return transform(json_df, context.execution_date.to_json() )

    @task()
    def load_task(json_df):
        load(pd.read_json(json_df))
    
    df= extract_task()
    clean_df = transform_task(df)
    load_task(clean_df)

# [START dag_invocation]
taskflow_api_etl_dag = taskflow_api_etl()
# [END dag_invocation]
