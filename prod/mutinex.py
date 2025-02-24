import pendulum
from airflow import DAG
from airflow import models
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

local_tz = pendulum.timezone("Australia/Sydney")
current_date = pendulum.now(local_tz)
prev_month_date = current_date.subtract(months=1)

prev_year = prev_month_date.year
prev_month = prev_month_date.month

start_date = prev_month_date.start_of('month').to_date_string()
end_date = prev_month_date.end_of('month').to_date_string()

DATA_SHARE_PROJECT = 'prod-data-sharing'
DATA_SHARE_DATASET = 'MUTINEX'
dag_owner = models.Variable.get ('dag_owner')

modes_cb_gcs = {
    'EXPORT_SALES_SET'          : 'sales', 
    'EXPORT_PRODUCTS_OFFERS'    : 'product_offers', 
    'EXPORT_DISCOUNTS'          : 'discounts', 
    'EXPORT_PRODUCT_PRICE'      : 'product_price'
}

modes_mut_gcs = {
    'EXPORT_SALES_SET'          : 'sales/sales', 
    'EXPORT_PRODUCTS_OFFERS'    : 'offers/offers', 
    'EXPORT_DISCOUNTS'          : 'discounts_new/discounts', 
    'EXPORT_PRODUCT_PRICE'      : 'pricing_new/pricing'
}

brands = {
    'Chicken Treat': 'ct',
    'Oporto': 'op',
    'Red Rooster': 'rr'
}

for brand in brands.keys():
    for mode in modes_cb_gcs.keys():
        print(brands[brand])

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

with DAG ("mutinex",
    schedule_interval = "0 6 6 * *",
    default_args = default_dag_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60)
    ) as dag:

    for brand in brands.keys():
        for mode in modes_cb_gcs.keys():

            task_group = f"{brand.replace(' ', '_').upper()}_{modes_cb_gcs[mode].upper()}"
            stored_proc_id = brand.replace(" ", "_").upper() + "_PROC"
            replicate_id = brand.replace(" ", "_").upper() + "_REPLICATE"

            with TaskGroup (group_id=task_group) as gcp:
                craveable_storage_dir = modes_cb_gcs[mode]
                mutiny_storage_dir = modes_mut_gcs[mode]
                brand_abbv = brands[brand]

                bash_commands = f"""
                    gcloud auth activate-service-account \
                        --project=prod-data-sharing \
                        --key-file=/home/airflow/gcs/data/craveables@mutiny.json 
                    gcloud auth list
                    gcloud config set account craveables@mutiny-gateway.iam.gserviceaccount.com

                    gcloud storage cp gs://mutinex/{brand_abbv}/{craveable_storage_dir}_{prev_year}_{prev_month:02d}_000000000000.csv \
                        gs://mutiny_craveables/{brand_abbv}/{mutiny_storage_dir}_{prev_year}_{prev_month:02d}.csv
                """

                gcs_dir = f"gs://mutinex/{brand_abbv}/{craveable_storage_dir}_{prev_year}_{prev_month:02d}_*.csv"

                run_stored_procedure = BigQueryExecuteQueryOperator(
                    task_id=stored_proc_id,
                    sql=f"""
                        DECLARE file_dir STRING;
                        DECLARE start_date DATE;
                        DECLARE end_date DATE;
                        SET file_dir = '{gcs_dir}';
                        SET start_date = '{start_date}';
                        SET end_date = '{end_date}';
                        CALL `{DATA_SHARE_PROJECT}.{DATA_SHARE_DATASET}.{mode}`('{brand}', file_dir, start_date, end_date);
                    """,
                    use_legacy_sql=False,
                    location="US",
                )

                run_gcloud_command = BashOperator(
                    task_id=replicate_id,
                    bash_command = bash_commands
                )

                run_stored_procedure >> run_gcloud_command