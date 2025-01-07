# import functions_framework
import os
import json
import requests
import logging
import uuid
import pytz
import traceback
import re
from datetime import datetime, timedelta, timezone, date
from google.cloud.exceptions import NotFound
import time
from multiprocessing import Pool
from multiprocessing import cpu_count
from google.cloud import bigquery
from google.cloud import secretmanager
from concurrent.futures import ThreadPoolExecutor  #concurrent calls to Oracle API 02-04-2024 by N.T

bigquery_client = bigquery.Client()
secret_client = secretmanager.SecretManagerServiceClient()

project_id_int = "683798020213"
token = "";
session = requests.Session()
apiAuthEndPoint = "https://qsrh-omra-idm.oracleindustry.com"
apiEndPoint = "https://qsrh-omra.oracleindustry.com"
username = "Craveable"
org = "CRV"
current_date = date.today().strftime("%Y-%m-%d")

successful_stores = 0 # used to count number of API calls to successful stores

sydney_tz = pytz.timezone('Australia/Sydney')
current_date_time = datetime.now(sydney_tz)#.strftime("%Y-%m-%d")
previous_date_time = current_date_time + timedelta(days=-1) #+ datetime.timedelta(days=1)
previous_date_time_7_days = current_date_time + timedelta(days=-7) #+ datetime.timedelta(days=1)
previous_date = previous_date_time.strftime("%Y-%m-%d")

# 16-Sep-2024 : given secre_id path, return value
def get_secret_data(project_id, secret_id):
    secret_detail = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_detail})
    data = response.payload.data.decode("UTF-8")
    return data

def WriteToBigQuery(apiName, arrayName, rsp, body, locName, busDt, mode):
    rows_to_insert = [];
    global successful_stores
    print(f"{apiName}: {rsp.status_code}")

    if mode == 'dataload':
        table_prefix = ''
        api_focus = 
    elif mode == 'backfill':
        table_prefix = 'BACKFILL_'
        api_focus = 'getOrderTypeDailyTotals'
    elif mode == 'daily_backfill':
        table_prefix = 'BACKFILL_'
        api_focus = 'getOrderTypeDailyTotals'

    try:
        if apiName == api_focus:
            if rsp.status_code == 200: # if the request to a store is successful for the getGuestChecks API, then iterate counter
                successful_stores += 1

        print(f'Received data calling api {apiName} for {locName} of {busDt}, Total Rows: {len(rsp.json()[arrayName])}')

        if len(rsp.json()[arrayName]) > 0:
            for survey in rsp.json()[arrayName]:
                # print("1: rows_to_insert.append")
                rows_to_insert.append({"payload": json.dumps(survey), "locRef": locName, "busDt": busDt});
            try:
                bigquery_client.get_table(f"dev01-insights.DEV_LND.{table_prefix}MICROSPOS_{apiName}_{current_date}")
            except NotFound:
                print(f'table not found for calling api {apiName} for {locName} of {busDt}, Total Rows: {len(rsp.json()[arrayName])}')
                table = bigquery.Table(f"dev01-insights.DEV_LND.{table_prefix}MICROSPOS_{apiName}_{current_date}", schema=[bigquery.SchemaField("payload", "STRING", mode="REQUIRED"), bigquery.SchemaField("locRef", "STRING", mode="REQUIRED"), bigquery.SchemaField("busDt", "STRING", mode="REQUIRED")])
                table = bigquery_client.create_table(table)
                time.sleep(45)
            
            try:
                errors = bigquery_client.insert_rows_json(f"dev01-insights.DEV_LND.{table_prefix}MICROSPOS_{apiName}_{current_date}", rows_to_insert);
                print(errors);
            except Exception as e:
                print(f"An error has occurred: {e})") 
                print(f"not able to insert {apiName}")
    except Exception as e:
        print(f"An error has occurred {e} after successful stores: {successful_stores}")
        if successful_stores == 0: # if the API wasn't able to hit any successful stores, then log an error
            if apiName == api_focus:
                traceback.print_exc()

def getToken(api, mode):
    global token

    cnc_string = get_secret_data(project_id_int, "CRAVEABLE-MICROSPOS")

    cnc_json = json.loads(cnc_string)

    client_id = cnc_json['CLIENT_ID']
    password = cnc_json['CLIENT_PWD']

    #Authorize
    rsp = session.get(f"{apiAuthEndPoint}/oidc-provider/v1/oauth2/authorize?client_id={client_id}&response_type=code&scope=openid&state=999&redirect_uri=apiaccount://callback&code_challenge=bfYBf5ZqgljYIxuqNrOFxThsVtrVtPi2PGhN3sphKTA&code_challenge_method=S256");

    print("response is: ", rsp)

    #Sign In
    form_data = {
        "username": username,
        "password": password,
        "orgname": org,
        "client_id": client_id
    }
    response = session.post(f"{apiAuthEndPoint}/oidc-provider/v1/oauth2/signin", data=form_data);
    rsp = response.json();

    #OAuth Token
    form_data = {
        "grant_type": "authorization_code",
        "code": rsp['redirectUrl'].split("?")[1].split("&")[0].replace("code=", ""),
        "client_id": client_id,
        "code_verifier": "ABCD1234567899874563ABCD1234567899874563ABC-SMT1.EMC-Client-242",
        "redirect_uri": "apiaccount://callback",
        "scope": "openid"
    }
    rsp = session.post(f"{apiAuthEndPoint}/oidc-provider/v1/oauth2/token", data=form_data);
    #print(rsp.json());
    token = rsp.json()['access_token']
    getLocations(api, mode);

def getLocations(api, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getLocationDimensions", headers=headers, json=form_data)
    allLocations = rsp.json()['locations']

    filtered_locations = [location for location in allLocations if re.match("^OP....|^OF....|^CT....|^RR....", location['locRef'])] # N.T parallel processing
    with ThreadPoolExecutor() as executor:  # parallel processing 01-03-2024 by N.T     
        executor.map(getHSTG, [(location, api, mode) for location in filtered_locations])

def getGuestChecks(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location,
        "opnBusDt": data_date,
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getGuestChecks", headers=headers, json=form_data)
    print(f"python function: def getGuestChecks | API Response Code: {rsp.status_code} | Location: {location['locRef']} | Data Date: {data_date}");
    WriteToBigQuery('getGuestChecks', 'guestChecks', rsp, form_data, location, data_date, mode)

def getOrderTypeDailyTotals(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getOrderTypeDailyTotals", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getOrderTypeDailyTotals', 'revenueCenters', rsp, form_data, location['locRef'], data_date, mode)

def getGuestChecks(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "opnBusDt": data_date,
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getGuestChecks", headers=headers, json=form_data)
    WriteToBigQuery('getGuestChecks', 'guestChecks', rsp, form_data, location['locRef'], data_date, mode)

def getDiscountDimensions(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getDiscountDimensions", headers=headers, json=form_data)
    WriteToBigQuery('getDiscountDimensions', 'discounts', rsp, form_data, location['locRef'], data_date, mode)

def getNonSalesTransactions(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date,
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getNonSalesTransactions", headers=headers, json=form_data)
    WriteToBigQuery('getNonSalesTransactions', 'nonSalesTransactions', rsp, form_data, location['locRef'], data_date, mode)

def getGuestCheckLineItemExtDetails(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date,
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getGuestCheckLineItemExtDetails", headers=headers, json=form_data)
    WriteToBigQuery('getGuestCheckLineItemExtDetails', 'revenueCenters', rsp, form_data, location['locRef'], data_date, mode)

def getMenuItemPrices(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getMenuItemPrices", headers=headers, json=form_data)
    WriteToBigQuery('getMenuItemPrices', 'menuItemPrices', rsp, form_data, location['locRef'], data_date, mode)

def getServiceChargeDimensions(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getServiceChargeDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getServiceChargeDimensions', 'serviceCharges', rsp, form_data, location['locRef'], data_date, mode)

def getServiceChargeDailyTotals(location, data_date, mode):
    #getServiceChargeDailyTotals
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getServiceChargeDailyTotals", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getServiceChargeDailyTotals', 'revenueCenters', rsp, form_data, location['locRef'], data_date, mode)

def getTaxDailyTotals(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getTaxDailyTotals", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getTaxDailyTotals', 'revenueCenters', rsp, form_data, location['locRef'], data_date, mode)

def getOrderTypeDimensions(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef']
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getOrderTypeDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getOrderTypeDimensions', 'orderTypes', rsp, form_data, location['locRef'], data_date, mode)

def getEmployeeDimensions(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef']
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getEmployeeDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getEmployeeDimensions', 'employees', rsp, form_data, location['locRef'], data_date, mode)

def getReasonCodeDimensions(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef']
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getReasonCodeDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getReasonCodeDimensions', 'reasonCodes', rsp, form_data, location['locRef'], data_date, mode)

def getCashierDimensions(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "include":"cashiers.num,cashiers.name,cashiers.mstrNum,cashiers.mstrName"
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getCashierDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getCashierDimensions', 'cashiers', rsp, form_data, location['locRef'], data_date, mode)

def getTaxDimensions(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getTaxDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getTaxDimensions', 'taxes', rsp, form_data, location['locRef'], data_date, mode)

def getTenderMediaDimensions(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getTenderMediaDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getTenderMediaDimensions', 'tenderMedias', rsp, form_data, location['locRef'], data_date, mode)

def getMenuItemDimensions(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getMenuItemDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getMenuItemDimensions', 'menuItems', rsp, form_data, location['locRef'], data_date, mode)

def getControlDailyTotals(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getControlDailyTotals", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getControlDailyTotals', 'revenueCenters', rsp, form_data, location['locRef'], data_date, mode)

def getControlDailyTotals(location, data_date, mode):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getControlDailyTotals", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getControlDailyTotals', 'revenueCenters', rsp, form_data, location['locRef'], data_date, mode)

#def getHSTG(location, apiName):
def getHSTG(location_api_name_tuple, mode):  # N.T 02-04-2024 added new function to pass Location and API as tuple
    location, apiName = location_api_name_tuple

    data_date = datetime.strptime("2022-12-06", '%Y-%m-%d').date()

    try:
        results = bigquery_client.query("SELECT MAX(SYS_SOURCE_TIMESTAMP) AS SYS_SOURCE_TIMESTAMP FROM dev01-insights.DEV_STG.HSTG_MICROSPOS_" + apiName.upper()).result();
        
        for row in results:
            if row.SYS_SOURCE_TIMESTAMP is None:
                data_date = datetime.strptime("2022-12-06", '%Y-%m-%d').date()

            else:
                data_date = row.SYS_SOURCE_TIMESTAMP + timedelta(days=1)

    except Exception as e:
        print(f"exception {e}");
        data_date = datetime.strptime("2022-12-06", '%Y-%m-%d').date()

    # the backfill/ingestion time changes depending on the mode of running DAG
    if mode == 'dataload':
        recency = 7
        data_date = current_date_time

        for single_date in daterange(previous_date_time, current_date_time):
            print (f'Fetching data for: {location} of') # {single_date.strftime("%Y-%m-%d")}');
            print(single_date.strftime("%Y-%m-%d"))
            if apiName == "getGuestChecks":
                getGuestChecks(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getNonSalesTransactions":
                getNonSalesTransactions(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getGuestCheckLineItemExtDetails":
                getGuestCheckLineItemExtDetails(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getMenuItemPrices":
                getMenuItemPrices(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getDiscountDimensions":
                getDiscountDimensions(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getServiceChargeDimensions":
                getServiceChargeDimensions(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getServiceChargeDailyTotals":
                getServiceChargeDailyTotals(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getTaxDailyTotals":
                getTaxDailyTotals(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getOrderTypeDailyTotals":
                getOrderTypeDailyTotals(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getOrderTypeDimensions":
                getOrderTypeDimensions(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getEmployeeDimensions":
                getEmployeeDimensions(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getReasonCodeDimensions":
                getReasonCodeDimensions(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getCashierDimensions":
                getCashierDimensions(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getTaxDimensions":
                getTaxDimensions(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getTenderMediaDimensions":
                getTenderMediaDimensions(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getMenuItemDimensions":
                getMenuItemDimensions(location, single_date.strftime("%Y-%m-%d"), mode);
            if apiName == "getOrderTypeDailyTotals90":
                getOrderTypeDailyTotals(location, (date.today() + timedelta(days=-90)).strftime("%Y-%m-%d"), mode);

    elif mode == 'backfill':
        recency = 30

    elif mode == 'daily_backfill':
        recency = 7

    if apiName.upper() == "GETORDERTYPEDAILYTOTALS":

        # data_date = datetime.now(sydney_tz).date()
        
        for single_date in daterange((data_date + timedelta(days=-recency)), data_date):
            print(f"getControlDailyTotals single_date: {single_date}")
            getControlDailyTotals(location, single_date.strftime("%Y-%m-%d"), mode);

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def main(request):
    #BELOW LINE NEEDS TO BE RECOMMENTED WHEN PUSHED
    api_name = request.form.get('api')
    mode_ = request.form.get('mode')

    print("calling api method: " + api_name)
    getToken(api_name, mode_);
    return "done."

if __name__ == "__main__":
    main("")
