""" This DAG computes the daily module conversion of web and app """
from datetime import datetime, timedelta

import os
import logging
import json
import datetime as dt
import boto3
import itertools

from pytz import timezone

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)
LOCAL_TZ = timezone('Australia/Sydney')
utc = timezone('UTC')
today = dt.datetime.now().replace(tzinfo=utc).astimezone(LOCAL_TZ)
yesterday = today - dt.timedelta(days=1)
LAST_SAVE_DATE = datetime.strptime(Variable.get("dag_start_date"), '%Y-%m-%d')

args = {
    'owner': 'singhg4n',
    'depends_on_past': False,
    'catchup': False,
    'start_date': LAST_SAVE_DATE,
    'email': ['xxx@yyy.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'provide_context': True,
    'retry_delay': timedelta(minutes=2)
}

PARENT_DAG_NAME = 'dag_creator'

dag_config = Variable.get("app_web_module_conversion_config", deserialize_json=True)


def get_last_modified_definitions(**context):
    s3 = boto3.resource('s3')
    previous_execution_date = context.get('prev_ds')
    bucket = s3.Bucket(Variable.get("sanitization_s3_sanit_def_files_folder").split('/')[0])
    list_modified_templates = []

    for file in itertools.chain(bucket.objects.filter(Prefix=dag_config['definition_config']),
                                bucket.objects.filter(Prefix=dag_config['parquet_definition_config']
                                                      )):
        if file.last_modified.strftime('%Y%m%d') >= previous_execution_date:
            list_modified_templates.append(file.key)

    context['task_instance'].xcom_push(key='recently_updated_templates',
                                       value=list_modified_templates)


def remove_create_dag(**context):
    s3 = S3Hook(aws_conn_id='s3_etl')
    airflow_dags_directory = context['conf'].get('core', 'dags_folder')
    list_definition_keys = context['task_instance'].xcom_pull(
        task_ids='get_last_modified_definitions', key='recently_updated_templates')

    if list_definition_keys:
        for definition_key in list_definition_keys:
            key_json = json.loads(s3.read_key(
                bucket_name=Variable.get("sanitization_s3_sanit_def_files_folder").split('/')[0],
                key=definition_key))
            logger.info(definition_key + 'Definition file is loaded successfully')

            landing_file_name = os.path.join(key_json.get('dag_folder'),
                                             key_json.get('dag_id') + ".py")
            dag_file_name = os.path.join(airflow_dags_directory, key_json.get('dag_id') + ".py")

            if os.path.exists(landing_file_name):
                os.unlink(dag_file_name)
                os.remove(landing_file_name)
                logger.info('DAG file is removed')

            try:
                with open(key_json.get('template_location_in_ec2'), 'r') as template_content:
                    content = template_content.read().replace('___DAG_ID___', key_json.get(
                        'dag_id'))
                    content = content.replace('___SCHEDULE_INTERVAL___', key_json.get(
                        'schedule_interval'))
                    content = content.replace('___TEMPLATE_JSON___', definition_key)
                    content = content.replace('___TABLE_NAME___', key_json.get('table_name'))
                    content = content.replace('___SCHEMA___', key_json.get('schema'))

            except Exception as e:
                raise AirflowException('Cannot open {template_ec2}'.format(
                    template_ec2=key_json.get('template_location_in_ec2')))

            with open(landing_file_name, 'w') as dag_file:
                dag_file.write(content)

            os.symlink(landing_file_name, dag_file_name)
            logger.info('DAG file is symbolic linked to the dag folder for definition' +
                        definition_key)
    else:
        logger.warning('No definition file was updated in s3!!Exiting...')


with DAG(PARENT_DAG_NAME, default_args=args, schedule_interval=None) as main_dag:
    doc_md = __doc__

    get_last_modified_templates_task = PythonOperator(
        task_id='get_last_modified_definitions',
        python_callable=get_last_modified_definitions,
        provide_context=True,
        retries=3
    )

    remove_create_dag_task = PythonOperator(
        task_id='remove_old_and_create_new_dag',
        python_callable=remove_create_dag,
        provide_context=True,
        retries=3
    )

    get_last_modified_templates_task >> remove_create_dag_task
