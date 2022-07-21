from asyncio import tasks
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup
from random import uniform
from datetime import datetime
import pandas as pd
from base_etl import extract, transform, load

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def transform_data(execution_date, **kwargs):
    ti= kwargs['ti']
    json_df = ti.xcom_pull(tasks_ids='extract_tasks')
    return transform(json_df, execution_date).to_json()

def load_data(**kwargs):
    ti= kwargs['ti']
    json_df = ti.xcom_pull(tasks_ids='transform_tasks')
    load(pd.read_json(json_df))

dag = DAG(
    'xcom',
    default_args=default_args,
    description='xcom share data into task machine learning',
)

push1 = PythonOperator(
    task_id='extract_task',provide_context=True, #provide context is for getting the TI (task instance ) parameters
    python_callable=extract,
    dag=dag,
    python_callable='extract_task',
)

push2 = PythonOperator(
    task_id='transform_task',provide_context=True, #provide context is for getting the TI (task instance ) parameters
    python_callable=transform,
    dag=dag,
    python_callable='transform_task',
)

push3 = PythonOperator(
    task_id='load_task',provide_context=True, #provide context is for getting the TI (task instance ) parameters
    python_callable=load,
    dag=dag,
    python_callable='load_task',
)

# set of operators, push1,push2 are upstream to pull
push1 >> push2 >> push3



# aplico redes neuronales para disparar una accion
def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')

    ti.xcom_push(key='model_accuracy', value=str(accuracy))

def _choose_best_model(**context):
    task_instance = context['task_instance']
    accuracies = task_instance.xcom_pull(key='model_accuracy', 
                              task_ids=['processing_tasks.training_model_a'
                                        'processing_tasks.training_model_b'
                                        'processing_tasks.training_model_c'])
    print(accuracies)

    acc_1 = task_instance.xcom_pull(key='model_accuracy', task_ids='processing_tasks.training_model_a')
    acc_2 = task_instance.xcom_pull(key='model_accuracy', task_ids='processing_tasks.training_model_b')
    acc_3 = task_instance.xcom_pull(key='model_accuracy', task_ids='processing_tasks.training_model_c')
    print(acc_1, acc_2, acc_3)
    print(type(acc_1))

    accuracies = [acc_1, acc_2, acc_3]
    print(accuracies)

with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3',
        do_xcom_push=False
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model,
            provide_context=True
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model,
            provide_context=True
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model,
            provide_context=True
        )

    choose_model = PythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model,
        provide_context=True
    )

    downloading_data >> processing_tasks >> choose_model