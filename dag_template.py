""" This DAG computes the daily module conversion of web and app """
import sys

sys.path.insert(0, "/home/airflow/airflow/plugins/")
sys.path.insert(0, "/opt/datascience/airflow-dags-social/plugins/")

from datetime import datetime, timedelta

import os
import boto3
import logging
import pandas as pd
import json
import datetime as dt

from pytz import timezone

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from s3_list_operator import S3ListOperator
from abc_redshift_operator import ABCRedshiftOperator
from s3_to_redshift_operator import S3ToRedshiftTransfer
from abc_redshift_to_s3_operator import ABCRedshiftToS3Transfer
from airflow.utils.trigger_rule import TriggerRule
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

PARENT_DAG_NAME = '___DAG_ID___'

dag_config = Variable.get("app_web_module_conversion_config", deserialize_json=True)
LOOK_BACK_DAYS = dt.datetime.now() - dt.timedelta(
    days=int(dag_config["module_conversion_look_back_days"]))


def load_definition(json_file, **context):
    """
    Loads the definition file from s3 and remove the required s3 files
    Args:
        json_file: Json definition file to load from s3

    """
    s3 = S3Hook(aws_conn_id='s3_etl')
    file_load = json.loads(s3.read_key(
        bucket_name=Variable.get('sanitization_s3_sanit_def_files_folder').split('/')[0],
        key=json_file))
    logger.info('Definition file is loaded successfully')

    try:
        dt.datetime.strptime(file_load.get('back_date_from'), '%Y-%m-%d')
        remove_s3_task = S3ListOperator(
            task_id='remove_s3',
            bucket=Variable.get('sanitization_s3_sanit_def_files_folder').split('/')[0],
            prefix='___SCHEMA___' + '/' + '___TABLE_NAME___' + '/',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            aws_conn_id='s3_etl',
            startafter='___SCHEMA___' + '/' + '___TABLE_NAME___' + '/batch_date=' +
                       file_load.get('back_date_from') + '/',
            retries=3
        )

        s3_keys = remove_s3_task.execute(context=context)

        if s3_keys:
            delete_s3_list = [s3_keys[file: file + int(dag_config["delete_key_chunk_size"])] for file in
                              range(0, len(s3_keys), int(dag_config["delete_key_chunk_size"]))]
            for s3_keys in delete_s3_list:
                s3.delete_objects(bucket=Variable.get('sanitization_s3_sanit_def_files_folder').split(
                    '/')[0], keys=s3_keys)

            delete_rows_task = ABCRedshiftOperator(
                task_id='delete_rows',
                source_name='___SCHEMA___',
                redshift_conn_id='snowplow_redshift',
                sql='delete from {table_name} where batch_date >= {back_date_from};'.format(
                    table_name=file_load.get('schema') + '.' + file_load.get('table_name'),
                    back_date_from=file_load.get('back_date_from')),
                retries=3
            )

            delete_rows_task.execute(context=context)
        else:
            logger.info('S3 and table are already backdated from the desired date!')

    except ValueError:
        logger.warning("Incorrect data format, should be YYYY-MM-DD. No keys will be deleted!!")

    context['task_instance'].xcom_push(key='query', value=file_load.get('query'))
    context['task_instance'].xcom_push(key='schema', value=file_load.get('schema'))
    context['task_instance'].xcom_push(key='table_name', value=file_load.get('table_name'))
    context['task_instance'].xcom_push(key='start_date', value=file_load.get('start_date'))
    context['task_instance'].xcom_push(key='table_columns', value=file_load.get('table_columns'))
    context['task_instance'].xcom_push(key='back_date_from', value=file_load.get('back_date_from'))
    context['task_instance'].xcom_push(key='batch_size', value=file_load.get('batch_size'))


def compute_next_gather(**context):
    """
    Computes the next date to run based on the s3 keys
    Args:
        **context: Airflow context

    Returns: The next dag id

    """
    next_gather_days = []
    latest_keys_from_s3 = context['task_instance'].xcom_pull(task_ids='list_s3')
    back_date_from = context['task_instance'].xcom_pull(task_ids='load_definition',
                                                        key='back_date_from')
    till_batch_date = yesterday.strftime('%Y-%m-%d')

    if latest_keys_from_s3:
        from_date = max([s3_keys.split('/')[-2].split('=')[1] for s3_keys in latest_keys_from_s3])
        if from_date >= till_batch_date:
            logger.info('Data is up to date!!!')
            return 'extract_last_batch_date'
    elif back_date_from and not latest_keys_from_s3:
        from_date = back_date_from
        next_gather_days.append(str(from_date))
    else:
        from_date = context['task_instance'].xcom_pull(task_ids='load_definition', key='start_date')
        next_gather_days.append(str(from_date))

    days_to_compute = (pd.to_datetime(till_batch_date, format='%Y-%m-%d') -
                       pd.to_datetime(from_date, format='%Y-%m-%d')).days

    logger.info('Number of days to compute: {days}'.format(days=days_to_compute))

    if isinstance(days_to_compute, int):
        for i in range(days_to_compute + 1):
            day_to_compute = (pd.to_datetime(from_date, format='%Y-%m-%d') +
                              timedelta(days=i + 1)).strftime('%Y-%m-%d')
            next_gather_days.append(day_to_compute)

    context['task_instance'].xcom_push(key='days_to_compute', value=next_gather_days)
    context['task_instance'].xcom_push(key='min_day', value=min(next_gather_days))
    context['task_instance'].xcom_push(key='max_day', value=max(next_gather_days))
    context['task_instance'].xcom_push(key='last_in_s3', value=from_date)
    return 'create_staging_table'


def run_queries(**context):
    """
    Run the aggregation queries
    Args:
        **context: Airflow context
    """
    query = context['task_instance'].xcom_pull(task_ids='load_definition', key='query')
    schema = context['task_instance'].xcom_pull(task_ids='load_definition', key='schema')
    table_name = context['task_instance'].xcom_pull(task_ids='load_definition', key='table_name')
    batch_size = int(context['task_instance'].xcom_pull(task_ids='load_definition',
                                                        key='batch_size'))
    from_date = pd.to_datetime(context['task_instance'].xcom_pull(task_ids='compute_next_gather',
                                                                  key='min_day'), format='%Y-%m-%d')
    to_date = pd.to_datetime(context['task_instance'].xcom_pull(task_ids='compute_next_gather', key='max_day'), format='%Y-%m-%d')
    total_days = (to_date - from_date).days
    logger.info('Total number of days are: {total_days}'.format(total_days=total_days))

    # by default batch_size is hardcoded
    if not batch_size:
        batch_size = 1

    if total_days > batch_size:
        logger.info('Total days are more then the batch size so chunking the dates in batch sizes '
                    'of {batch_size}!'.format(batch_size=batch_size))
        from_dates = [(from_date + timedelta(days=i * batch_size)).strftime('%Y-%m-%d %H:%M:%S')
                      for i in range(0, int(total_days/batch_size))]
        to_dates = [(from_date + timedelta(days=i * batch_size)).strftime('%Y-%m-%d %H:%M:%S')
                    for i in range(1, int(total_days/batch_size) + 1)]
        del to_dates[-1]
        to_dates.append(to_date)
    else:
        from_dates = [from_date]
        to_dates = [to_date]

    logger.info('Queries to be executed for batches: ')
    # logger.info(from_dates)
    # logger.info(to_dates)
    for from_date, to_date in zip(from_dates, to_dates):
        logger.info(query.format(schema=schema, table_name='sta_' + table_name, from_date=from_date,
                                 to_date=to_date))

    truncate_staging_task = ABCRedshiftOperator(
        task_id='truncate_staging',
        source_name='___SCHEMA___',
        redshift_conn_id='snowplow_redshift',
        sql='truncate {schema}.{table_name};'.format(schema=schema,
                                                     table_name='sta_' + table_name),
        retries=3
    )

    truncate_staging_task.execute(context=context)

    for from_date, to_date in zip(from_dates, to_dates):
        aggregate_data_task = ABCRedshiftOperator(
            task_id='aggregate_data',
            source_name='___SCHEMA___',
            redshift_conn_id='snowplow_redshift',
            sql=query.format(schema=schema, table_name='sta_' + table_name, from_date=from_date,
                             to_date=to_date),
            retries=3
        )

        aggregate_data_task.execute(context=context)


def unload_from_staging(**context):
    """
    Unload the staging table into s3 into the required folder
    Args:
        **context: Airflow context
    """
    s3 = S3Hook(aws_conn_id='s3_etl')
    days_computed = context['task_instance'].xcom_pull(
        task_ids='compute_next_gather', key='days_to_compute')
    schema = context['task_instance'].xcom_pull(task_ids='load_definition', key='schema')
    table_name = context['task_instance'].xcom_pull(task_ids='load_definition', key='table_name')

    for batch_date in sorted(days_computed)[:-1]:
        s3_keys = s3.list_keys(bucket_name=Variable.get('sanitization_s3_sanit_def_files_folder').split('/')[0],
                               prefix='___SCHEMA___' + '/' + table_name + '/batch_date=' +
                                      batch_date + '/')
        if s3_keys:
            s3.delete_objects(bucket=Variable.get('sanitization_s3_sanit_def_files_folder').split('/')[0],
                              keys=s3_keys)

        unload_task = ABCRedshiftToS3Transfer(
            task_id='unload',
            redshift_conn_id='snowplow_redshift',
            aws_conn_id='s3_etl',
            schema=schema,
            table='sta_' + table_name,
            s3_key='___SCHEMA___',
            s3_unload_path='s3://' + Variable.get('sanitization_s3_sanit_def_files_folder').split(
                '/')[0] + '/' + '___SCHEMA___' + '/' + table_name + '/batch_date=' + batch_date +
            '/',
            s3_bucket=Variable.get('sanitization_s3_sanit_def_files_folder').split('/')[0],
            where_clause='batch_date = \'' + batch_date + '\'',
            # where_clause='batch_date >= \'' + batch_date + ' 00:00:00\' and batch_date <= \'' +
            #              (pd.to_datetime(batch_date, format='%Y-%m-%d') + timedelta(
            #                  days=1)).strftime('%Y-%m-%d') + ' 00:00:00\'',
            unload_options=dag_config["module_conversion_unload_options"],
            retries=3
        )

        unload_task.execute(context=context)

    drop_staging = ABCRedshiftOperator(
        task_id='drop_staging',
        source_name='___SCHEMA___',
        redshift_conn_id='snowplow_redshift',
        sql='drop table if exists {staging_table};'.format(
            staging_table=schema + '.sta_' + table_name),
        retries=3
    )

    drop_staging.execute(context=context)


def extract_last_batch_date(**context):
    """
    Computes the last batch_date from the table
    Args:
        **context: Airflow context
    """
    schema = context['task_instance'].xcom_pull(task_ids='load_definition', key='schema')
    table_name = context['task_instance'].xcom_pull(task_ids='load_definition', key='table_name')
    
    current_batch_date_task = ABCRedshiftOperator(
        task_id='current_batch_date',
        source_name='___SCHEMA___',
        redshift_conn_id='snowplow_redshift',
        sql='select max({batch_col}) from {target};'.format(
            batch_col='batch_date', target=schema + '.' + table_name),
        xcom_push=True,
        retries=3
    )

    current_batch_date = current_batch_date_task.execute(context=context)

    if current_batch_date[0][0]:
        context['task_instance'].xcom_push(key='last_batch', value=current_batch_date[0][0])
        context['task_instance'].xcom_push(key='last_batch_for_table_check',
                                           value=current_batch_date[0][0])
    else:
        context['task_instance'].xcom_push(
            key='last_batch', value=(context['task_instance'].xcom_pull(
                task_ids='load_definition', key='start_date')))
        context['task_instance'].xcom_push(
            key='last_batch_for_table_check', value=(pd.to_datetime(context[
                'task_instance'].xcom_pull(
                task_ids='load_definition', key='start_date'), format='%Y-%m-%d') - timedelta(
                days=1)).strftime('%Y-%m-%d'))


def s3_to_final(**context):
    """
    Copy statement from s3 into final table
    Args:
        **context: Airflow context
    """
    list_s3_for_table = context['task_instance'].xcom_pull(task_ids='list_s3_for_table')
    table_name = context['task_instance'].xcom_pull(task_ids='load_definition', key='table_name')
    table_columns = context['task_instance'].xcom_pull(task_ids='load_definition', 
                                                       key='table_columns')
    schema = context['task_instance'].xcom_pull(task_ids='load_definition', key='schema')
    last_batch = context['task_instance'].xcom_pull(task_ids='extract_last_batch_date',
                                                    key='last_batch_for_table_check')
    list_s3_for_table = [s3_key for s3_key in list_s3_for_table
                         if s3_key.split('/')[-2].split('=')[1] > str(pd.to_datetime(last_batch))]
    list_s3_for_table = sorted(list(set(['/'.join(s3_key.split('/')[:-1]) + '/' for s3_key in
                                         list_s3_for_table])))

    if list_s3_for_table:
        logger.info('Number of keys to push to Redshift are: {count}'.format(count=len(
            list_s3_for_table)))

        to_final_task = S3ToRedshiftTransfer(
            task_id='to_final',
            schema=schema,
            table=table_name,
            s3_bucket=Variable.get('sanitization_s3_sanit_def_files_folder').split('/')[0],
            s3_key=list_s3_for_table,
            aws_conn_id='s3_etl',
            cols=table_columns,
            redshift_conn_id='snowplow_redshift',
            is_truncate=False,
            copy_options=dag_config["module_conversion_copy_options"]
        )
        to_final_task.execute(context=context)
    else:
        logger.info('Table is up to date!! No need to push data to it!!')


def vacuum_analyse_redshift(table_name, **context):
    """
    Vacuum & analyse the required table
    Args:
        table_name: Name of the table for analyze
        **context: airflow context
    """
    if context['execution_date'].weekday() == 0 or context['execution_date'].weekday() == 2 or \
            context['execution_date'].weekday() == 4:
        pg_hook = PostgresHook(postgres_conn_id='snowplow_redshift')
        pg_hook.run("vacuum full {table_name}; analyze {table_name};".format(
            table_name=table_name), autocommit=True)
        logger.info('Redshift Vacuum and analyze executed successfully for table: ' + table_name)


with DAG(PARENT_DAG_NAME, default_args=args, schedule_interval='___SCHEDULE_INTERVAL___',
         max_active_runs=1) as main_dag:
    doc_md = __doc__

    load_definition_task = PythonOperator(
        task_id='load_definition',
        python_callable=load_definition,
        op_args=['___TEMPLATE_JSON___'],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        provide_context=True,
        retries=3
    )

    list_s3_task = S3ListOperator(
        task_id='list_s3',
        bucket=Variable.get('sanitization_s3_sanit_def_files_folder').split('/')[0],
        prefix='___SCHEMA___' + '/' + '___TABLE_NAME___' + '/',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        aws_conn_id='s3_etl',
        startafter='___SCHEMA___' + '/' + '___TABLE_NAME___' + '/batch_date=' +
        LOOK_BACK_DAYS.strftime("%Y-%m-%d") + '/',
        retries=3
    )

    compute_next_gather_task = BranchPythonOperator(
        task_id='compute_next_gather',
        python_callable=compute_next_gather,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        retries=3
    )

    create_staging_table_task = ABCRedshiftOperator(
        task_id='create_staging_table',
        source_name='___SCHEMA___',
        redshift_conn_id='snowplow_redshift',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        sql='drop table if exists {staging_table}; create table {staging_table} '
            '(like {final_table});'.format(
            staging_table="{{ task_instance.xcom_pull(task_ids='load_definition', "
                          "key='schema')}}" + '.sta_' + '___TABLE_NAME___',
            final_table="{{ task_instance.xcom_pull(task_ids='load_definition', key='schema')}}" + 
                        '.' + '___TABLE_NAME___'),
        retries=3
    )

    run_queries_task = PythonOperator(
        task_id='run_queries',
        python_callable=run_queries,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        provide_context=True,
        retries=10
    )

    unload_staging_task = PythonOperator(
        task_id='unload_staging',
        python_callable=unload_from_staging,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        provide_context=True,
        retries=5
    )

    extract_last_batch_date_task = PythonOperator(
        task_id='extract_last_batch_date',
        python_callable=extract_last_batch_date,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=3
    )

    list_s3_for_table_task = S3ListOperator(
        task_id='list_s3_for_table',
        bucket=Variable.get('sanitization_s3_sanit_def_files_folder').split('/')[0],
        prefix='___SCHEMA___' + '/' + '___TABLE_NAME___' + '/',
        aws_conn_id='s3_etl',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        startafter='___SCHEMA___' + '/' + '___TABLE_NAME___' + '/batch_date=' +
        "{{ task_instance.xcom_pull(task_ids='extract_last_batch_date', key='last_batch')}}" + '/',
        retries=3
    )

    s3_to_final_task = PythonOperator(
        task_id='s3_to_final',
        python_callable=s3_to_final,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        retries=3
    )

    redshift_vacuum_analyze_task = PythonOperator(
        task_id='redshift_vacuum_analyze',
        python_callable=vacuum_analyse_redshift,
        provide_context=True,
        op_args=["{{ task_instance.xcom_pull(task_ids='load_definition', "
                 "key='schema')}}" + '.' + '___TABLE_NAME___']
    )

    load_definition_task >> list_s3_task >> compute_next_gather_task >> \
        create_staging_table_task >> run_queries_task >> unload_staging_task >> \
        extract_last_batch_date_task >> list_s3_for_table_task >> s3_to_final_task >> \
        redshift_vacuum_analyze_task
    compute_next_gather_task >> extract_last_batch_date_task
