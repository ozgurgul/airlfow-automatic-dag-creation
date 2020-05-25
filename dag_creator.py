""" This DAG computes the daily module conversion of web and app """
from datetime import datetime, timedelta

import os
import logging
import json
import datetime as dt
#import boto3
#import itertools

from pytz import timezone

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable

from datetime import datetime, timedelta
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)
LOCAL_TZ = timezone('Europe/London')
utc = timezone('UTC')
today = dt.datetime.now().replace(tzinfo=utc).astimezone(LOCAL_TZ)
yesterday = today - dt.timedelta(days=1)
LAST_SAVE_DATE = datetime.strptime(Variable.get("dag_start_date_time"), '%Y-%m-%d %H:%M:%S')

args = {
    'owner': 'voa_user',
    'depends_on_past': False,
    'catchup': False,
    'start_date': LAST_SAVE_DATE,
    'email': ['ozgur.gul@hpe.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'provide_context': True,
    'retry_delay': timedelta(minutes=2)
}

PARENT_DAG_NAME = 'dag_creator'

dag_config = Variable.get("app_web_module_conversion_config", deserialize_json=True)


def get_last_modified_definitions(**context):
    
    previous_execution_date = context.get('prev_ds')    
    query = """
            SELECT *
            FROM voa.iv_evitem
            WHERE insert_time::date >= date '{}'
            AND insert_time::date < date '{}'
    """.format(previous_execution_date, today.strftime('%Y-%m-%d %H:%S:%M')) # .strftime('%Y-%m-%d')

    src_conn = PostgresHook(postgres_conn_id='source',
                            schema='source_schema').get_conn()

    # notice this time we are naming the cursor for the origin table
    # that's going to force the library to create a server cursor
    src_cursor = src_conn.cursor("serverCursor")
    src_cursor.execute(query)
    
        # now we need to iterate over the cursor to get the records in batches
    while True:
        records = src_cursor.fetchmany(size=2000)
        if not records:
            break
       

    src_cursor.close()
    
    
    #bucket = s3.Bucket(Variable.get("sanitization_s3_sanit_def_files_folder").split('/')[0])
    
    list_modified_transcripts = []

    for row in records:
        if row.insert_time.strftime('%Y%m%d %H:%M:%S') >= previous_execution_date:
            list_modified_transcripts.append(row.uid)

    context['task_instance'].xcom_push(key='recently_updated_transcripts',
                                       value=list_modified_transcripts)


def remove_create_dag(**context):
    #s3 = S3Hook(aws_conn_id='s3_etl')
    
    execution_date = context.get('execution_date').strftime('%Y-%m-%d')
    query = """
            SELECT *
            FROM voa.va_stg_voice
            WHERE created_at::date = date '{}'
    """.format(execution_date)

    src_conn = PostgresHook(postgres_conn_id='source',
                            schema='source_schema').get_conn()
    dest_conn = PostgresHook(postgres_conn_id='dest',
                             schema='dest_schema').get_conn()

    # notice this time we are naming the cursor for the origin table
    # that's going to force the library to create a server cursor
    src_cursor = src_conn.cursor("serverCursor")
    src_cursor.execute(query)
    dest_cursor = dest_conn.cursor()


    
    
    airflow_dags_directory = context['conf'].get('core', 'dags_folder')
    
    list_transcript_keys = context['task_instance'].xcom_pull(
        task_ids='get_last_modified_transcripts', key='recently_updated_transcripts')

    if list_transcript_keys:
        for transcript_key in list_transcript_keys:
            key_json = json.loads(s3.read_key(
                bucket_name=Variable.get("sanitization_s3_sanit_def_files_folder").split('/')[0],
                key=definition_key))
            logger.info(transcript_key + 'Transcript record is loaded successfully')

            landing_file_name = os.path.join(key_json.get('dag_folder'),
                                             key_json.get('dag_id') + ".py")
            dag_file_name = os.path.join(airflow_dags_directory, key_json.get('dag_id') + ".py")

            if os.path.exists(landing_file_name):
                os.unlink(dag_file_name)
                os.remove(landing_file_name)
                logger.info('DAG file is removed')

            try:
                with open(key_json.get('template_location_in_server'), 'r') as template_content:
                    content = template_content.read().replace('___DAG_ID___', key_json.get(
                        'dag_id'))
                    content = content.replace('___SCHEDULE_INTERVAL___', key_json.get(
                        'schedule_interval'))
                    content = content.replace('___TEMPLATE_JSON___', definition_key)
                    content = content.replace('___TABLE_NAME___', key_json.get('table_name'))
                    content = content.replace('___SCHEMA___', key_json.get('schema'))

            except Exception as e:
                raise AirflowException('Cannot open {template_server}'.format(
                    template_server=key_json.get('template_location_in_server')))

            with open(landing_file_name, 'w') as dag_file:
                dag_file.write(content)

            os.symlink(landing_file_name, dag_file_name)
            logger.info('DAG file is symbolic linked to the dag folder for definition' +
                        transcription_key)
    else:
        logger.warning('No transcription file was updated in server!!Exiting...')


with DAG(PARENT_DAG_NAME, default_args=args, schedule_interval=None) as main_dag:
    doc_md = __doc__

    get_last_modified_transcript_task = PythonOperator(
        task_id='get_last_modified_transcripts',
        python_callable=get_last_modified_transcripts,
        provide_context=True,
        retries=3
    )

    remove_create_dag_task = PythonOperator(
        task_id='remove_old_and_create_new_dag',
        python_callable=remove_create_dag,
        provide_context=True,
        retries=3
    )

    get_last_modified_transcripts_task >> remove_create_dag_task
