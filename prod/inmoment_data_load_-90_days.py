import pendulum
from airflow import DAG
from airflow import models
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import  BigQueryHook
from airflow.hooks.http_hook import HttpHook
#import datetime
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.oauth2 import id_token
import google.auth.transport.requests
import traceback
import requests
import socket
from urllib3.connection import HTTPConnection

local_tz = pendulum.timezone("Australia/Sydney")

BQ_CONN_ID = models.Variable.get ('bq_connection')
BQ_PROJECT = models.Variable.get ('gcp_project')
dag_owner = models.Variable.get ('dag_owner')
source_system_code = "InMoment"
dlf_batch_name = ""

def dlf_pre_execute (system_code, package_type, package_name):
    print(package_name);
    return 0;

def dlf_on_failure_logging (context):	    
    task = context.get('task_instance').task_id
    ti = context.get('task_instance')
    package_name = task.split('.')[0]

    exception = context.get('exception')
    formatted_exception = ''.join(traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)).strip()

    #dlf_post_execute (package_name, "FAILURE", formatted_exception, ti)

def dlf_execute_stored_procedure (stored_procedure, ti):
    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    #bq_client = bigquery.Client(project = hook._get_field("project"), credentials = hook._get_credentials())
    bq_client = hook.get_client(project_id=hook._get_field("project"))
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

def call_cloud_function():

    #get ID TOKEN
    #http = HttpHook("POST", http_conn_id="inmoment_survey_90_days")
    #hostname = http.get_connection("inmoment_survey_90_days").host
    #target_audience = hostname 
    request = google.auth.transport.requests.Request()
    idt = id_token.fetch_id_token(request, "https://us-central1-prod-insights.cloudfunctions.net/inmoment-survey-90-days")
    headers = { 'Authorization' : "Bearer " + idt }

    #connection to cloud function killed when runtime > 5-10 minutes
    #below code prevents connection from closing automatically
    # SO_KEEPALIVE: 1 => Enable TCP keepalive
    # TCP_KEEPIDLE: 60 => Time in seconds until the first keepalive is sent
    # TCP_KEEPINTVL: 60 => How often should the keepalive packet be sent
    # TCP_KEEPCNT: 100 => The max number of keepalive packets to send
    HTTPConnection.default_socket_options += [
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60),
        (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 60),
        (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 60),
    ]
    
    response = requests.post("https://us-central1-prod-insights.cloudfunctions.net/inmoment-survey-90-days",data={'process_name': 'survey'},headers=headers)
    return response.text

# DAG / Task / Group Definitions
default_dag_args = {
    "owner": dag_owner,
    'start_date': datetime(2023, 7, 1, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    "emails": ['dhaval.faria@one51.com.au'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "depends_on_past": False
}

with DAG ("inmoment_data_load_-90_days",
    schedule_interval = "0 8 * * 7",
    default_args = default_dag_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60)
    ) as dag:

#    survey_response_load = GCPCloudFunctionOperator (
#        task_id = "survey_response",
#        method = "POST",
#        http_conn_id = "inmoment_survey_90_days",
#        data={'process_name': 'survey'},
#        endpoint='/',
#        headers={},
#        response_check=lambda response: True if response.status_code == 200 is True else False,
#        on_failure_callback=dlf_on_failure_logging
#    )

    survey_response_load = PythonOperator(
        task_id="survey_response",
        python_callable=call_cloud_function
    )

    survey_load_stg = PythonOperator  (
        task_id = "inmoment_survey_load_stg",
        python_callable = dlf_execute_stored_procedure,
        op_kwargs = {
            'stored_procedure' : "STG.LOAD_STG_INMOMENT_SURVEY_RESPONSE"
        },
        on_failure_callback=dlf_on_failure_logging
    )

    survey_load_hstg = PythonOperator  (
        task_id = "inmoment_survey_load_hstg",
        python_callable = dlf_execute_stored_procedure,
        op_kwargs = {
            'stored_procedure' : "HSTG.LOAD_HSTG_INMOMENT_SURVEY_RESPONSE"
        },
        on_failure_callback=dlf_on_failure_logging
    )

survey_response_load >> survey_load_stg >> survey_load_hstg