import pendulum
from airflow import DAG
from airflow import models
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import  BigQueryHook
from airflow.hooks.http_hook import HttpHook
#import datetime
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.oauth2 import id_token
import google.auth.transport.requests
import json
import traceback

# Variable Definitions
local_tz = pendulum.timezone("Australia/Sydney")

BQ_CONN_ID = models.Variable.get ('bq_connection')
BQ_PROJECT = models.Variable.get ('gcp_project')
dag_owner = models.Variable.get ('dag_owner')
source_system_code = "UBER_EATS"
dlf_batch_name = source_system_code + "_DATA_LOAD_BATCH"

report_types = models.Variable.get ('uber_eats_report_types', deserialize_json=True)

days_to_extract = models.Variable.get('uber_eats_days_to_extract')

task_group_prefix = "LND_STG_HSTG_"
lnd_prefix = "LND.LOAD_LND_" + source_system_code + "_"
stg_prefix = "STG.LOAD_STG_" + source_system_code + "_"
hstg_prefix = "HSTG.LOAD_HSTG_" + source_system_code + "_"

# Python Function Definitions

def dlf_pre_execute (system_code, package_type, package_name, ti):
    if (package_type=="MODULE"):
        batch_execution_id = ti.xcom_pull(key=dlf_batch_name)
    else:
        batch_execution_id = None

    # hook = BigQueryHook(bigquery_conn_id=BQ_CONN_ID, use_legacy_sql=False) # P.D. Removed 2023-09-05
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    # bq_client = bigquery.Client(project = hook._get_field("project"), credentials = hook._get_credentials()) # P.D. Removed 2023-09-05
    bq_client = hook.get_client(project_id = hook._get_field("project"))
    query = """
    DECLARE v_EXECUTION_ID, v_BATCH_EXECUTION_ID, v_SYSTEM_CODE, v_PACKAGE_NAME, v_PACKAGE_TYPE, v_RETURN_MESSAGE STRING;
    SET v_SYSTEM_CODE = '{0}';
    SET v_PACKAGE_TYPE = '{1}';   
    SET v_PACKAGE_NAME = '{2}';  
    SET v_BATCH_EXECUTION_ID = '{3}'; 
    CALL `{4}.CFG.PRE_EXECUTION`(v_PACKAGE_NAME, v_BATCH_EXECUTION_ID, v_SYSTEM_CODE, NULL, NULL, v_PACKAGE_TYPE, v_EXECUTION_ID);
    SELECT v_EXECUTION_ID AS EXECUTION_ID;
    """.format(system_code, package_type, package_name, batch_execution_id, BQ_PROJECT)
    job = bq_client.query(query)
    
    for result in job.result():
        execution_id = result.EXECUTION_ID
        ti.xcom_push(key=package_name, value=execution_id)
        # print (execution_id)
    
    return execution_id

def dlf_post_execute (package_name, execution_status, error_message, ti):
    execution_id = ti.xcom_pull(key=package_name)
    # hook = BigQueryHook(bigquery_conn_id=BQ_CONN_ID, use_legacy_sql=False) # P.D. Removed 2023-09-05
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    # bq_client = bigquery.Client(project = hook._get_field("project"), credentials = hook._get_credentials()) # P.D. Removed 2023-09-05
    bq_client = hook.get_client(project_id = hook._get_field("project"))
    query = """
    CALL `{3}.CFG.POST_EXECUTION`('{0}', '{1}', '{2}');
    """.format(
            execution_id,
            execution_status,
            error_message,
            BQ_PROJECT
        )    
    bq_client.query(query)

def dlf_execute_stored_procedure (stored_procedure, ti):
    # hook = BigQueryHook(bigquery_conn_id=BQ_CONN_ID, use_legacy_sql=False) # P.D. Removed 2023-09-05
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    # bq_client = bigquery.Client(project = hook._get_field("project"), credentials = hook._get_credentials()) # P.D. Removed 2023-09-05
    bq_client = hook.get_client(project_id = hook._get_field("project"))
    query = """
    DECLARE v_RETURN_MESSAGE STRING;
    CALL `{0}.{1}` (v_RETURN_MESSAGE);
    SELECT v_RETURN_MESSAGE AS RETURN_MESSAGE;   
    """.format(
            BQ_PROJECT,
            stored_procedure
        )
    job = bq_client.query(query)

    for result in job.result():
        retmsg = result.RETURN_MESSAGE
        print (retmsg)    

def dlf_on_failure_logging (context):	    
    task = context.get('task_instance').task_id
    ti = context.get('task_instance')
    package_name = task.split('.')[0]

    exception = context.get('exception')
    formatted_exception = ''.join(traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)).strip()

    dlf_post_execute (package_name, "FAILURE", formatted_exception, ti)

# DAG / Task / Group Definitions
default_dag_args = {
    "owner": dag_owner,
    'start_date': datetime(2022, 10, 25, 6, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    "emails": ['julio.acuna@one51.com.au'],
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    "depends_on_past": False
}

with DAG ("ubereats_data_load",
    schedule_interval = "30 7 * * *",
    default_args = default_dag_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60)
    ) as dag:

    data_load_batch_start = PythonOperator (
        task_id = "batch_start",
        python_callable = dlf_pre_execute,
        op_kwargs = {
            'system_code' : source_system_code, 
            'package_type' : "BATCH",
            'package_name' : dlf_batch_name
        },
        provide_context=True
    )

    data_load_package = source_system_code + '_DATA_LOAD'

    with TaskGroup(group_id=data_load_package) as data_load_process:

        # Dynamically create tasks according to the table array
        for report_type in report_types:
            task_group = task_group_prefix + report_type
            lnd_table = lnd_prefix + report_type
            stg_table = stg_prefix + report_type
            hstg_table = hstg_prefix + report_type

            with TaskGroup (group_id=task_group) as STG_HSTG:

                data_load_module_start = PythonOperator (
                    task_id = "module_start",
                    python_callable = dlf_pre_execute,
                    op_kwargs = {
                        'system_code' : source_system_code,
                        'package_type' : "MODULE", 
                        'package_name' : task_group
                    },
                    provide_context=True
                )
            
                data_load_lnd = PythonOperator (
                    task_id = lnd_table,
                    python_callable = dlf_execute_stored_procedure,
                    op_kwargs = {
                        'stored_procedure' : lnd_table
                    },
                    on_failure_callback=dlf_on_failure_logging
                )
            
                data_load_stg = PythonOperator (
                    task_id = stg_table,
                    python_callable = dlf_execute_stored_procedure,
                    op_kwargs = {
                        'stored_procedure' : stg_table
                    },
                    on_failure_callback=dlf_on_failure_logging
                )

                data_load_hstg = PythonOperator (
                    task_id = hstg_table,
                    python_callable = dlf_execute_stored_procedure,
                    op_kwargs = {
                        'stored_procedure' : hstg_table
                    },
                    on_failure_callback=dlf_on_failure_logging
                )

                data_load_module_end = PythonOperator (
                    task_id = "module_end",
                    python_callable = dlf_post_execute,
                    op_kwargs = {
                        'package_name' : task_group,
                        'execution_status' : "SUCCESS",
                        'error_message': None
                    },
                    # trigger_rule='all_success',
                    provide_context=True
                )            

                data_load_module_start >> data_load_lnd >> data_load_stg >> data_load_hstg >> data_load_module_end

    data_load_batch_end = PythonOperator (
        task_id = "batch_end",
        python_callable = dlf_post_execute,
        op_kwargs = {
            'package_name' : dlf_batch_name,
            'execution_status' : "SUCCESS",
            'error_message': None
        },
        # trigger_rule='all_success',
        provide_context=True
    )

    data_load_batch_error = PythonOperator (
        task_id = "batch_error",
        python_callable = dlf_post_execute,
        op_kwargs = {
            'package_name' : dlf_batch_name,
            'execution_status' : "FAILURE",
            'error_message': "Terminated with failure."
        },
        trigger_rule='one_failed',
        provide_context=True
    )

data_load_batch_start >> data_load_process >> data_load_batch_end
data_load_process >> data_load_batch_error