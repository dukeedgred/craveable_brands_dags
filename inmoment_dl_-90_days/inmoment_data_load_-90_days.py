import pendulum
from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import  BigQueryHook
from airflow.hooks.base import BaseHook

#import datetime
from datetime import datetime, timedelta
from google.oauth2 import id_token
import google.auth.transport.requests
import traceback
import requests
import socket
from urllib3.connection import HTTPConnection
import logging

local_tz = pendulum.timezone("Australia/Sydney")

##############################################################
BQ_CONN_ID = models.Variable.get ('dev_bq_connection')
BQ_PROJECT = models.Variable.get ('dev_gcp_project')
##############################################################

# BQ_CONN_ID = models.Variable.get ('bq_connection')
# BQ_PROJECT = models.Variable.get ('gcp_project')
dag_owner = models.Variable.get ('dag_owner')


def dlf_on_failure_logging (context):	    
    task = context.get('task_instance').task_id
    ti = context.get('task_instance')
    package_name = task.split('.')[0]

    exception = context.get('exception')
    formatted_exception = ''.join(traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)).strip()

def dlf_execute_stored_procedure (stored_procedure, ti):

    hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)

    project_id = hook._get_field('project')

    bq_client = hook.get_client(project_id=project_id)

    query = """
    DECLARE v_RETURN_MESSAGE STRING;
    CALL `{0}.{1}` (v_RETURN_MESSAGE);
    SELECT v_RETURN_MESSAGE AS RETURN_MESSAGE;   
    """.format(
            BQ_PROJECT,
            stored_procedure
        )
    job = bq_client.query(query)

    logging.info("query executed")

    for result in job.result():
        retmsg = result.RETURN_MESSAGE
        print (retmsg)

def call_cloud_function():

    request = google.auth.transport.requests.Request()
    idt = id_token.fetch_id_token(request, "https://us-central1-dev01-insights.cloudfunctions.net/dev-inmoment-survey-90-days")
    headers = { 'Authorization' : "Bearer " + idt }

    logging.info(headers)

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
    
    response = requests.post(
        "https://us-central1-dev01-insights.cloudfunctions.net/dev-inmoment-survey-90-days", 
        data={'process_name': 'survey'},
        headers=headers
    )
    return response.text

# DAG / Task / Group Definitions
default_dag_args = {
    "owner": dag_owner,
    'start_date': datetime(2023, 7, 1, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    "emails": ['pervendren.naidoo@craveablebrands.com'],
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

    survey_response_load = PythonOperator(
        task_id="survey_response",
        python_callable=call_cloud_function
    )

    survey_load_stg = PythonOperator  (
        task_id = "inmoment_survey_load_stg",
        python_callable = dlf_execute_stored_procedure,
        op_kwargs = {
            'stored_procedure' : "DEV_STG.LOAD_STG_INMOMENT_SURVEY_RESPONSE"
        },
        on_failure_callback=dlf_on_failure_logging
    )

    survey_load_hstg = PythonOperator  (
        task_id = "inmoment_survey_load_hstg",
        python_callable = dlf_execute_stored_procedure,
        op_kwargs = {
            'stored_procedure' : "DEV_STG.LOAD_HSTG_INMOMENT_SURVEY_RESPONSE"
        },
        on_failure_callback=dlf_on_failure_logging
    )

survey_response_load >> survey_load_stg >> survey_load_hstg

