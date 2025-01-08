import pendulum
from airflow import DAG
from airflow import models
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import  BigQueryHook
from airflow.hooks.http_hook import HttpHook
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.oauth2 import id_token
import google.auth.transport.requests
import json
import requests
import traceback
import logging

# Variable Definitions
local_tz = pendulum.timezone("Australia/Sydney")
#yesterday = datetime.today() - timedelta(days=1) #always export the day before
export_date = local_tz.convert(datetime.today()) #localize the date

BQ_CONN_ID = models.Variable.get ('bq_connection')
BQ_PROJECT = models.Variable.get ('gcp_project')
BQ_PROJECT_ECOMM = models.Variable.get ('gcp_project_ecomm')
dag_owner = models.Variable.get ('dag_owner')
source_system_code = "LOYALTY"
dlf_batch_name = source_system_code + "_DATA_LOAD_BATCH"

brand_list = ["op","rr"]
find_unenriched_proc = "ECOMM_XX_APPS_DELIVERECT.usp_find_unenriched_transactid"
update_unenriched_proc = "ECOMM_XX_APPS_DELIVERECT.usp_update_unenriched_transactid"

logger = logging.getLogger(__name__)

#================= begin declarations ================================================

# Calling CF in GCP
class GCPCloudFunctionOperator(SimpleHttpOperator):
    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        hostname = http.get_connection(self.http_conn_id).host
        self.log.info(f'Calling HTTP method: {hostname}.')
        target_audience = hostname 
        request = google.auth.transport.requests.Request()
        idt = id_token.fetch_id_token(request, target_audience)
        self.headers = { 'Authorization' : 'Bearer ' + idt, 'Content-Type': 'application/json' }
        
        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        self.log.info(response)

        if response.status_code == 200:
            return True
        else:
            return False

def dlf_pre_execute (system_code, package_type, package_name, ti):
    if (package_type=="MODULE"):
        batch_execution_id = ti.xcom_pull(key=dlf_batch_name)
    else:
        batch_execution_id = None

    hook = BigQueryHook(gcp_conn_id =BQ_CONN_ID, use_legacy_sql=False)
    bq_client = hook.get_client(project_id=hook._get_field("project"))
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
    
    return execution_id

# expects the project & stored_procedure name which is (dataset + proc name)
def dlf_execute_stored_procedure (bq_project, stored_procedure, ti):
    hook = BigQueryHook(gcp_conn_id =BQ_CONN_ID, use_legacy_sql=False)
    bq_client = hook.get_client(project_id=hook._get_field("project"))
    query = """
    DECLARE v_RETURN_MESSAGE STRING;
    CALL `{0}.{1}` (v_RETURN_MESSAGE);
    SELECT v_RETURN_MESSAGE AS RETURN_MESSAGE;   
    """.format(
            bq_project,
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

def dlf_post_execute (package_name, execution_status, error_message, ti):
    execution_id = ti.xcom_pull(key=package_name)
    hook = BigQueryHook(gcp_conn_id =BQ_CONN_ID, use_legacy_sql=False)
    bq_client = hook.get_client(project_id=hook._get_field("project"))
    query = """
    CALL `{3}.CFG.POST_EXECUTION`('{0}', '{1}', '{2}');
    """.format(
            execution_id,
            execution_status,
            error_message,
            BQ_PROJECT
        )    
    bq_client.query(query)    

#================= end declarations ===================================================

# DAG / Task / Group Definitions
default_dag_args = {
    "owner": dag_owner,
    'start_date': datetime(2024, 2, 27, 15, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    "emails": ['eddie.chong@craveablebrands.com.au'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "depends_on_past": False
}

with DAG ("loyalty_daily_matchback_process",
    schedule_interval = "0 7 * * *",
    default_args = default_dag_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=90)
    ) as dag:

    processing_batch_start = PythonOperator (
        task_id = "batch_start",
        python_callable = dlf_pre_execute,
        op_kwargs = {
            'system_code' : source_system_code, 
            'package_type' : "BATCH",
            'package_name' : dlf_batch_name
        },
        provide_context=True
    )

    processing_batch_end = PythonOperator (
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

    processing_batch_error = PythonOperator (
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

    with TaskGroup(group_id="matchback_processing") as matchback_processing:

        for brand in brand_list:

            task_group = f"PROCESSING_BRAND_{brand.upper()}"

            with TaskGroup (group_id=task_group) as STG_HSTG:

                processing_module_start = PythonOperator (
                    task_id = "module_start",
                    python_callable = dlf_pre_execute,
                    op_kwargs = {
                        'system_code' : source_system_code,
                        'package_type' : "MODULE", 
                        'package_name' : task_group
                    },
                    provide_context=True
                )

                processing_module_end = PythonOperator (
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

                find_unenriched = PythonOperator (
                    task_id = f"FIND_UNENRICHED_{brand.upper()}_SP",
                    python_callable = dlf_execute_stored_procedure,
                    op_kwargs = {
                        'bq_project' : BQ_PROJECT_ECOMM,
                        'stored_procedure' : find_unenriched_proc.replace("XX", brand.upper())
                    },
                    on_failure_callback=dlf_on_failure_logging
                )        

                # https://prod-dw-etl-ecom-enrich-transactorid-nowvpwp6oq-uc.a.run.app
                enrich_transactorid = GCPCloudFunctionOperator(
                    task_id = f"UNENRICHED_TRANSACTORID_{brand.upper()}_CF",
                    method='POST',
                    http_conn_id='gcp_cf_dw_etl_loyalty_enrich_transactid',
                    data=json.dumps({'brand': brand}),
                    endpoint='/',
                    headers={},
                    response_check=lambda response: True if response.status_code == 200 is True else False,
                    on_failure_callback=dlf_on_failure_logging
                )

                update_unenriched = PythonOperator (
                    task_id = f"UPDATE_UNENRICHED_{brand.upper()}_SP",
                    python_callable = dlf_execute_stored_procedure,
                    op_kwargs = {
                        'bq_project' : BQ_PROJECT_ECOMM,
                        'stored_procedure' : update_unenriched_proc.replace("XX", brand.upper())
                    },
                    on_failure_callback=dlf_on_failure_logging
                )

                # https://prod-dw-etl-ecom-export-matchback-to-sftp-nowvpwp6oq-uc.a.run.app
                export_sftp = GCPCloudFunctionOperator(
                    task_id = f"EXPORT_MATCHBACK_{brand.upper()}_CF",
                    method='POST',
                    http_conn_id='gcp_cf_dw_etl_loyalty_export_sftp',
                    data=json.dumps({'brand': brand, 'export_date': str(export_date.date()) }) , 
                    endpoint='/',
                    headers={},
                    response_check=lambda response: True if response.status_code == 200 is True else False,
                    on_failure_callback=dlf_on_failure_logging
                )            

                processing_module_start >> find_unenriched >> enrich_transactorid >> update_unenriched >> export_sftp >> processing_module_end


processing_batch_start >> matchback_processing >> processing_batch_end
matchback_processing >> processing_batch_error