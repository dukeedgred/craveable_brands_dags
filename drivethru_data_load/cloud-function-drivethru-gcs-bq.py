import functions_framework
import logging
import pytz
import datetime
import os
import json

from google.cloud import error_reporting
from google.cloud import bigquery
from google.cloud import storage

storage_client = storage.Client()
error_client = error_reporting.Client()
bigquery_client = bigquery.Client()

# Get the environmental variables
cloud_storage_name =  os.environ['drivethru_bucket']
schema_bucket = os.environ['schema_bucket']
source_dataset = os.environ['source_dataset']
schema_file_name = os.environ['report_name']

local_timezone = pytz.timezone('Australia/Sydney');
# local_timestamp = datetime.datetime.now(local_timezone).strftime('%Y%m%d');
local_date = datetime.datetime.now(local_timezone).strftime('%Y%m%d');


def load_data_from_GCS_to_BQ(gcs_file_location, target_table_name, file_type, disposition, dataset_name, schema_bucket, schema_file_name):
    table_ref = bigquery_client.dataset(dataset_name).table(target_table_name)

    # Build the job configuration
    job_config = bigquery.LoadJobConfig()

    if(file_type == 'rc1'):
        job_config.source_format = bigquery.SourceFormat.CSV 
        job_config.allow_quoted_newlines = True
        job_config.field_delimiter ='\t'
        job_config.skip_leading_rows = 1
    else:
        # Log the error
        logging.error(f'Error: Occured as the {gcs_file_location} is not .rc1 file type')
        error_client.report(f'Error: Occured as the {gcs_file_location} is not .rc1 file type')
    
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
        destination_table.expires = datetime.datetime.now() + datetime.timedelta(days=7)
        bigquery_client.update_table(destination_table, ['expires'])

        print(f'After execution, table {target_table_name} has {destination_table.num_rows} rows')

    except Exception as error:
        logging.error(f'Error: Occured while loading file {gcs_file_location} to BigQuery and the exception is {error}')
        error_client.report(f'Error: Occured while loading file {gcs_file_location} to BigQuery and the exception is {error}')

# Method to list all the files inside a Google Cloud Bucket
def list_blobs(bucket_name, prefix):

    files_list = []

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    for blob in blobs:
        files_list.append(blob)

    print (f"Total files found : {len(files_list)}")

    return files_list        

def try_one_file(bucket_name, prefix):

    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter="/")

    unique_folders = list(blobs.prefixes)

    files_from_folders = []

    for folder in unique_folders:
        print(f"Processing folder: {folder}")
        folder_blobs = storage_client.list_blobs(bucket_name, prefix=folder)

        # Get the first file in the folder
        for file_blob in folder_blobs:
            print(f"Selected file: {file_blob.name}")
            files_from_folders.append((folder, file_blob.name))
            break  # Only pick one file from the folder

    print(f"Total files selected: {len(files_from_folders)}")

    return files_from_folders


@functions_framework.http
def get_files_from_gcs_to_bq(request):


    ######################################################################
    # print(f"probing {cloud_storage_name} {local_date}/RR")
    # files_list = list_blobs(cloud_storage_name, local_date + "/RR") # Just remove /RR from this
    files_list = try_one_file(cloud_storage_name, local_date)
    ######################################################################

    print(files_list)

    # if len(files_list) != 0:
    #     for file in files_list:

    #         if file != "":
    #             # Print out the data from bucket, to prove that it worked
    #             print(f'Running for file {file.name}')

    #             gcs_file_location = f'gs://{cloud_storage_name}/{file.name}'
    #             brand = file.name.split('/')[1] # ie OP, RR
    #             filename = file.name.split('/')[2]
    #             filename_without_ext = filename.split('.')[0].replace(" ", "_")
    #             # BQ table name consists of filename_fileextracteddate
    #             # file_date = filename_without_ext[12:20]
    #             tablename = f'DRIVE_THRU_{local_date}_{brand}_{filename_without_ext}' # update GCF

    #             filetype = filename.split('.')[1].lower()
    #             drivethru_filetype = filename.split('.')[1].lstrip()
    #             drivethru_groupname = filename.split('.')[0].lstrip().replace(" ", "_")
    #             drivethru_type = f'{drivethru_groupname}_{drivethru_filetype}'
                

    #         # # Only process files with the .rc1 extension
    #         if drivethru_filetype == 'rc1':
    #             load_data_from_GCS_to_BQ(gcs_file_location, tablename, filetype, 'WRITE_TRUNCATE', source_dataset, schema_bucket, schema_file_name )

    # else:
    #     print(f'There is no file {cloud_storage_name} to be processed')

    return 'OK'