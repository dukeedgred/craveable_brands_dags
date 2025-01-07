import functions_framework
import os
import json
import requests
from datetime import date, timedelta, datetime
import time
import logging
import pytz

from google.cloud.exceptions import NotFound
from google.cloud import storage
from google.cloud import error_reporting
from google.cloud import pubsub_v1
from google.cloud import kms
from google.cloud import bigquery

# Get environmental Variables
# Ubereats URL for authentication
URL_AUTH = os.environ['url_auth']
# Ubereats URLs for listing all stores
URL_LIST_STORES = os.environ['url_list_stores']
# Ubereats URL for requesting reports
URL_REPORT = os.environ['url_report']

# Get the credentials
CLIENT_ID = os.environ['client_id']
CLIENT_ID_ENC = os.environ['client_id_enc']
CLIENT_SECRET = os.environ['client_secret']
CLIENT_SECRET_ENC = os.environ['client_secret_enc']

# Initializing clients
storage_client = storage.Client()
error_client = error_reporting.Client()
publisher_client = pubsub_v1.PublisherClient()
kms_client = kms.KeyManagementServiceClient()
bigquery_client = bigquery.Client()

# Decrypt client_id and client_string using KMS
client_id = '2-oJpFgfO_gBnPAo9dqSKljrHBfzX1Xv'
client_secret = 'J_qRiD4WrDwJzx0JSfstD5uJzh258RqwcWC72dxt'
local_timezone = pytz.timezone('Australia/Sydney')
local_timestamp = datetime.now(local_timezone).strftime('%Y%m%d_%H%M%S')


@functions_framework.http
def main(request):
    try:
        days_to_extract = int(request.form.get('days_to_extract'))
        report_types = json.loads(request.form.get('report_types'))    
        stores_batch = int(os.environ['stores_batch'])

        # ==============  Authenticate API ==============
        # Build the request parameters
        payload = f'client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials&scope=eats.report eats.store'
        headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
        # Send a request to get the access token
        auth_response = requests.request("POST", URL_AUTH, headers=headers, data=payload)

        if auth_response.ok:
            uber_eats_token = auth_response.json()['access_token']
            print(f'Store Token retrieved successfully: {uber_eats_token}')
        else:
            # Log the error
            logging.error(f'Error: Unable to get the store access token and the error is {auth_response.raise_for_status()}')
            error_client.report(f'Error: Unable to get the store access token and the error is {auth_response.raise_for_status()}')


        # ==============  Get Stores ==============    
        # Request stores
        stores = get_store_ids (URL_LIST_STORES, uber_eats_token)
        print (f"# of stores:" + str(len(stores)))

        if not stores:
            # Log the error
            logging.error(f'No stores retrieved, exiting process')
            error_client.report(f'No stores retrieved, exiting process')

        start_timedelta = days_to_extract + 2
        start_date = f'{date.today()-timedelta(start_timedelta):%Y-%m-%d}'
        end_date = f'{date.today()-timedelta(2):%Y-%m-%d}'

        report_types = ["ORDERS_AND_ITEMS_REPORT", "ORDER_ERRORS_TRANSACTION_REPORT", "ORDER_ERRORS_MENU_ITEM_REPORT", "ORDER_HISTORY_REPORT", "CUSTOMER_AND_DELIVERY_FEEDBACK_REPORT", "MENU_ITEM_FEEDBACK_REPORT", "DOWNTIME_REPORT"]

        print (f"Date range: {start_date} to {end_date}")
        
        for i in range(0,len(stores),stores_batch):
            stores_runlist = stores[i:i+stores_batch]

            print (f"# of stores_runlist:" + str(len(stores_runlist)))
            print ("store_id(s): "+ ', '.join(stores_runlist))
            
            for report_type in report_types:
                # Wait before API call 
                time.sleep(5)  

                # Request report
                post_report_request(URL_REPORT, report_type, uber_eats_token, stores_runlist, start_date, end_date)

    except Exception as error:
        # Log the error
        logging.error(f'Error: {error}')
        error_client.report(f'Error: {error}')
        raise ValueError(error)

    return '{"status":"200", "data": "OK"}'


def get_store_ids(url, token):   
    
    payload={}
    headers={'Authorization': 'Bearer ' +  token}   
    
    # Hit Stores API
    response = requests.request("GET", url, headers=headers, data=payload)
    print(response.text)
    stores_list = json.loads(response.text)["stores"]

    print (stores_list)

    # write stores to bigquery
    insert_stores_to_bigquery(stores_list);
    
    # store_ids = [store["store_id"] for store in stores_list if store["pos_data"]["integration_enabled"] == True]
    store_ids = [store["store_id"] for store in stores_list]

    # Query to paginate api if next_key is present in initial API response      
    url_with_start_key = url + '?start_key=' 
    
    while ('next_key' in response.json().keys()):        
        next_key = json.loads(response.text)["next_key"]
        print ("NEXT_KEY: ",next_key)       
        url = url_with_start_key + next_key
        print("URL: ",url)       
        response = requests.request("GET", url, headers=headers, data=payload)        
        stores_list = json.loads(response.text)["stores"] 
        print("LEN: ",len(stores_list))       
        # write stores to bigquery
        insert_stores_to_bigquery(stores_list);
        # store_ids.extend(store["store_id"] for store in stores_list if store["pos_data"]["integration_enabled"] == True)
        store_ids.extend(store["store_id"] for store in stores_list)

    return store_ids

def insert_stores_to_bigquery(stores):
    rows_to_insert = [];
    for store in stores:
        rows_to_insert.append({"payload": json.dumps(store)});

    try:
        bigquery_client.get_table(f"prod-insights.LND_RAW.UBER_EATS_STORES_{local_timestamp}")
    except NotFound:
        print(f"prod-insights.LND_RAW.UBER_EATS_STORES_{local_timestamp} table not found, Creating one.")
        table = bigquery.Table(f"prod-insights.LND_RAW.UBER_EATS_STORES_{local_timestamp}", schema=[bigquery.SchemaField("payload", "STRING", mode="REQUIRED")])
        table = bigquery_client.create_table(table)
        time.sleep(10)

    try:
        errors = bigquery_client.insert_rows_json(f"prod-insights.LND_RAW.UBER_EATS_STORES_{local_timestamp}", rows_to_insert);
    except Exception as e:
        print(e)
        print(f"not able to insert Stores.")

def post_report_request(url, report_type, token, store_uuids, start_date, end_date):

    # 20240327 (EC) : for "ORDER_ERRORS_TRANSACTION_REPORT", make the start date -6 and end date -4 because UE API can only return endate older than 4 days
    if (report_type == "ORDER_ERRORS_TRANSACTION_REPORT"):
        date_in_sydney = datetime.now(pytz.timezone('Australia/Sydney')).date()
        start_date = f'{date_in_sydney - timedelta(7):%Y-%m-%d}'
        end_date = f'{date_in_sydney - timedelta(5):%Y-%m-%d}'
        
    payload_dict = {"report_type": report_type,
                    "store_uuids": store_uuids,
                    "start_date": start_date,
                    "end_date": end_date}
    
    payload = json.dumps(payload_dict)    
    
    auth_header = 'Bearer '+ token

    headers = {
          'Authorization': auth_header,
          'Content-Type': 'application/json'
        }

    # Hit Report API
    print("CALLING REPORT API : ", url,"|",report_type,"|",store_uuids,"|",start_date,"|",end_date,"|")
    response = requests.request("POST", url, headers=headers, data=payload)
    print("FINISH CALLING REPORT API")
    print("PRINTING RESPONSE")
    print(response.json())
    print("FINISH PRINTING RESPONSE")

    # workflow_id = response.json()['workflow_id']
    # print(f'Workflow ID for {report_type} is {workflow_id}')
    
    return response.text