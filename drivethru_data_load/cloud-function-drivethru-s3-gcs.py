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
import json
from datetime import timedelta

from google.cloud import error_reporting
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import secretmanager

aws_key_sm = os.environ['AWS_KEY_SECRET_MANAGER']
google_storage_name = os.environ['google_storage_bucket'] 

secret_client = secretmanager.SecretManagerServiceClient()
storage_client = storage.Client()
error_client = error_reporting.Client()

bucket_gcs = storage_client.get_bucket(google_storage_name)
bigquery_client = bigquery.Client()

aws_key_json_response = secret_client.access_secret_version(name=aws_key_sm)
aws_key_json_string = aws_key_json_response.payload.data.decode("UTF-8")

aws_key_json = json.loads(aws_key_json_string)

aws_key = aws_key_json['AWS_KEY']
aws_secret = aws_key_json['AWS_SECRET']

s3_client = boto3.client('s3',aws_access_key_id=aws_key.strip('\n'),aws_secret_access_key=aws_secret.strip('\n'), region_name = os.environ['s3_region_name']) 
s3_bucket_name = os.environ['s3_bucket_name']
s3 = boto3.resource('s3',aws_access_key_id=aws_key.strip('\n'),aws_secret_access_key=aws_secret.strip('\n'), region_name = os.environ['s3_region_name']) 
my_bucket=s3.Bucket(s3_bucket_name)

print("connection established to aws bucket.")

@functions_framework.http
def get_files_from_s3_to_gcs(request):

    report_type = request.form.get("report_type")

    print(report_type)
    print("received request for report type: " + report_type)

    local_timezone = pytz.timezone('Australia/Sydney')
    local_timestamp = datetime.datetime.now(local_timezone).strftime('%Y%m%d')
    file_timestamp = datetime.datetime.now(local_timezone).strftime('%Y%d%m')

    print("downloading report for " + report_type + " of: " + local_timestamp)

    suffix_filename = f'cardata{file_timestamp} 3.rc1'

    allfiles = my_bucket.objects.all()

    for obj in allfiles:
        if obj.key.endswith(suffix_filename) == True:
            # key = obj.key
            filename = obj.key
            body = obj.get()['Body'].read()

            data_stream = io.BytesIO()
            s3_client.download_fileobj(
                Bucket=s3_bucket_name,
                Key=filename,
                Fileobj=data_stream
            )     

            try:
                # Upload to cloud storage
                blob = bucket_gcs.blob(local_timestamp + '/' + filename) 
                blob.upload_from_file(data_stream, rewind=True)   

                gcs_file_location = 'gs://'+ google_storage_name + '/' + local_timestamp + '/' + filename;     
                print(f'File uploaded successfully to {gcs_file_location}');

            # Log the error
            except Exception as error:  
                logging.error(f'Error: Occured while uploading file {gcs_file_location} to GCS and the exception is {error}')
                error_client.report(f'Error: Occured while uploading file {gcs_file_location} to GCS and the exception is {error}')                
                
    return '{"status":"200", "data": "OK"}'
