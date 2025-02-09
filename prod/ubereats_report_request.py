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
report_type_list = []
for report in report_types:
    if report != "STORES":
        report_type_list.append(report)
report_type_listx = json.dumps(report_type_list)  

days_to_extract = models.Variable.get('uber_eats_days_to_extract')

# Python Function Definitions
# Data Ingestion
class GCPCloudFunctionOperator(HttpOperator):
    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        hostname = http.get_connection(self.http_conn_id).host
        self.log.info(f'Calling HTTP method: {hostname}.')
        target_audience = hostname
        request = google.auth.transport.requests.Request()
        idt = id_token.fetch_id_token(request, target_audience)
        self.headers = { 'Authorization': f"Bearer {idt}" }
        response = http.run(
            self.endpoint,
            self.data,
            self.headers,
            self.extra_options
        )

        self.log.info(response)

        return response.status_code == 200

def dlf_pre_execute (system_code, package_type, package_name, ti):
    if (package_type=="MODULE"):
        batch_execution_id = ti.xcom_pull(key=dlf_batch_name)
    else:
        batch_execution_id = None

    #hook = BigQueryHook(bigquery_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    #bq_client = bigquery.Client(project = hook._get_field("project"), credentials = hook._get_credentials())
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
    #hook = BigQueryHook(bigquery_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    #bq_client = bigquery.Client(project = hook._get_field("project"), credentials = hook._get_credentials())
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
    #hook = BigQueryHook(bigquery_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    #bq_client = bigquery.Client(project = hook._get_field("project"), credentials = hook._get_credentials())
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
    'start_date': datetime(2023, 9, 11, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    "emails": ['julio.acuna@one51.com.au'],
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    "depends_on_past": False
}

with DAG ("ubereats_report_request",
    schedule_interval = "0 6 * * *",
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

    data_ingestion_package = source_system_code + '_DATA_INGESTION'

    with TaskGroup(group_id=data_ingestion_package) as data_ingestion:

        data_ingestion_module_start = PythonOperator (
            task_id = "module_start",
            python_callable = dlf_pre_execute,
            op_kwargs = {
                'system_code' : source_system_code,
                'package_type' : "MODULE", 
                'package_name' : data_ingestion_package
            },
            provide_context=True
        )

        ubereats_load = GCPCloudFunctionOperator (
            task_id = "ubereats_request_report",
            method = "POST",
            http_conn_id = "gcp_cf_dw_etl_ubereats_0_reports_request",
            data={'days_to_extract': days_to_extract, 'report_types': report_type_listx},
            endpoint='/',
            headers={},
            response_check=lambda response: True if response.status_code == 200 is True else False,
            on_failure_callback=dlf_on_failure_logging
        )

        data_ingestion_module_end = PythonOperator (
            task_id = "module_end",
            python_callable = dlf_post_execute,
            op_kwargs = {
                'package_name' : data_ingestion_package,
                'execution_status' : "SUCCESS",
                'error_message': None
            },
            # trigger_rule='all_success',
            provide_context=True
        )    

        data_ingestion_module_start >> ubereats_load >> data_ingestion_module_end    

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

data_load_batch_start >> data_ingestion >> data_load_batch_end
data_ingestion >> data_load_batch_error