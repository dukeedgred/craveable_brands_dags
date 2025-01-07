import functions_framework
import boto3
from boto3.s3.transfer import TransferConfig
import pytz
import datetime
import time
import io
import os
import logging
import base64
from datetime import timedelta

from google.cloud import error_reporting
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import secretmanager

secret_client = secretmanager.SecretManagerServiceClient()

aws_key_sm = os.environ['AWS_KEY_SECRET_MANAGER']

aws_key_json_response = secret_client.access_secret_version(name=aws_key_sm)
aws_key_json_string = aws_key_json_response.payload.data.decode("UTF-8")

aws_key_json = json.loads(aws_key_json_string)

aws_key = aws_key_json['AWS_KEY']
aws_secret = aws_key_json['AWS_SECRET']

cloud_storage_name = os.environ['storage_bucket'] #"dev-dw-etl-menulog"

storage_client = storage.Client()
error_client = error_reporting.Client()
bucket_gcs = storage_client.get_bucket(cloud_storage_name)
bigquery_client = bigquery.Client()

s3_client = boto3.client('s3',aws_access_key_id=aws_key.strip('\n'),aws_secret_access_key=aws_secret.strip('\n'),region_name = os.environ['s3_region_name'])
s3_bucket_name=os.environ['s3_bucket_name']
s3 = boto3.resource('s3',aws_access_key_id=aws_key.strip('\n'),aws_secret_access_key=aws_secret.strip('\n'),region_name = os.environ['s3_region_name'])
my_bucket=s3.Bucket(s3_bucket_name)

print("connection established to aws bucket.")

def load_data_from_GCS_to_BQ(gcs_file_location, target_table_name, file_type, disposition, dataset_name, schema_bucket, schema_file_name):
    table_ref = bigquery_client.dataset(dataset_name).table(target_table_name)

    # Build the job configuration
    job_config = bigquery.LoadJobConfig()

    if(file_type == '.csv'):
        job_config.source_format = bigquery.SourceFormat.CSV 
        job_config.allow_quoted_newlines = True
        job_config.field_delimiter =','
        job_config.skip_leading_rows = 1
    else:
        # Log the error
        logging.error(f'Error: Occured as the {gcs_file_location} is not .CSV file type')
        error_client.report(f'Error: Occured as the {gcs_file_location} is not .CSV file type')
    
    # If disposition is append  allow automatic schema update option.
    if disposition == 'WRITE_APPEND':
        job_config.schema_update_options = ['ALLOW_FIELD_ADDITION']
    
    job_config.ignore_unknown_values = True
    job_config.create_disposition = 'CREATE_IF_NEEDED',
    job_config.write_disposition = disposition,
    uri = gcs_file_location

    # If schema is defined, pick the schema from GCS
    if schema_file_name:

        schema_source = schema_file_name
        schema_destination = '/tmp/' + schema_file_name

        bucket = storage_client.bucket(schema_bucket)
        blob = bucket.blob(schema_source)
        blob.download_to_filename(schema_destination)

        schema_fields = bigquery_client.schema_from_json(schema_destination)
        job_config.schema = schema_fields

    else:
       job_config.autodetect = True

    try:
        load_job = bigquery_client.load_table_from_uri(uri, table_ref, job_config=job_config)  # API request

        print(f'Starting job {load_job.job_id}')
        print(f'Job executing for file in GCS location {gcs_file_location}')

        # Waits for table load to complete
        load_job.result()
        print('Job finished')
        # Print the total number of rows
        destination_table = bigquery_client.get_table(table_ref)
        print(f'After execution, table {target_table_name} has {destination_table.num_rows} rows')
    except Exception as error:
        logging.error(f'Error: Occured while loading file {gcs_file_location} to BigQuery and the exception is {error}')
        error_client.report(f'Error: Occured while loading file {gcs_file_location} to BigQuery and the exception is {error}')

@functions_framework.http
def get_files_from_s3(request):
    print("request received: " + request.form.get("report_type"));
    report_type = request.form.get("report_type");
    print("received request for report type: " + request.form.get("report_type"));

    local_timestamp = "";

    bg_result = bigquery_client.query("SELECT MAX(SYS_SOURCE_TIMESTAMP) AS SYS_SOURCE_TIMESTAMP FROM `HSTG.HSTG_MENULOG_" + report_type + "`");

    local_timestamp = (bg_result[0].SYS_SOURCE_TIMESTAMP + timedelta(days=7)).strftime('%Y-%m-%d')

    # row = next(iter(bg_result))  # Safely handles generators/iterables
    # local_timestamp = (row.SYS_SOURCE_TIMESTAMP + timedelta(days=7)).strftime('%Y-%m-%d')

    print("downloading report for " + report_type + " of: " + local_timestamp);
    
    allfiles = my_bucket.objects.filter(Prefix=report_type.lower() + "_" + local_timestamp);
    for obj in allfiles:
        print(obj.key)
        key = obj.key
        body = obj.get()['Body'].read()

        data_stream = io.BytesIO()
        s3_client.download_fileobj(
            Bucket=s3_bucket_name,
            Key=key,
            Fileobj=data_stream
        )

        #Upload to cloud storage
        blob = bucket_gcs.blob(key)
        blob.upload_from_file(data_stream, rewind=True)

        filelocation = "gs://" + os.environ['storage_bucket'] + "/" + key;
        #tbl_name = key.split("local_timestamp")
        print(filelocation);

        if "order_details_report" in key:
            load_data_from_GCS_to_BQ(filelocation, 'MENULOG_' + key.replace(".csv", ""), '.csv', 'WRITE_APPEND', 'DEV_STG', 'dev-dw-etl-menulog-schemas', 'order_details_report.json')
        elif "order_items_report" in key:
            load_data_from_GCS_to_BQ(filelocation, 'MENULOG_' + key.replace(".csv", ""), '.csv', 'WRITE_APPEND', 'DEV_STG', 'dev-dw-etl-menulog-schemas', 'order_items_report.json')
        break
    return '{"status":"200", "data": "OK"}'

