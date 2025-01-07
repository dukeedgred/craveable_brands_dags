# import functions_framework
import os
import json
import requests
import logging
import uuid
import pytz
import traceback
from datetime import datetime, timedelta, timezone, date
from google.cloud.exceptions import NotFound
import time
from datetime import datetime
from multiprocessing import Pool
from multiprocessing import cpu_count
from google.cloud import bigquery
from google.cloud import secretmanager
from concurrent.futures import ThreadPoolExecutor  #concurrent calls to Oracle API 20-02-2024 by N.T


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

sydney_tz = pytz.timezone('Australia/Sydney')
current_datetime = datetime.now(sydney_tz).strftime("%Y-%m-%d_%H-15-00")

successful_stores = 0 # used to count number of API calls to successful stores

# 16-Sep-2024 : given secre_id path, return value
def get_secret_data(project_id, secret_id):
    secret_detail = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_detail})
    data = response.payload.data.decode("UTF-8")
    return data

def WriteToBigQuery(apiName, arrayName, rsp, body, locName, busDt):
    rows_to_insert = [];
    global successful_stores

    print(f"{apiName}: {rsp.status_code}")
    try:
        if apiName == 'getGuestChecks':
            if rsp.status_code == 200:  # if the request to a store is successful for the getGuestChecks API, then iterate counter
                successful_stores += 1

        print(f'Received data calling api {apiName} for {locName} of {busDt}, Total Rows: {len(rsp.json()[arrayName])}')
        if len(rsp.json()[arrayName]) > 0:
            for survey in rsp.json()[arrayName]:
                # print("1")
                rows_to_insert.append({"payload": json.dumps(survey), "locRef": locName, "busDt": busDt});
            try:
                # print("2")
                bigquery_client.get_table(f"dev01-insights.DEV_LND.HOURLY_MICROSPOS_{apiName}_{current_datetime}")
            except NotFound:
                print(f'table not found for calling api {apiName} for {locName} of {busDt}, Total Rows: {len(rsp.json()[arrayName])}')
                table = bigquery.Table(f"dev01-insights.DEV_LND.HOURLY_MICROSPOS_{apiName}_{current_datetime}", schema=[bigquery.SchemaField("payload", "STRING", mode="REQUIRED"), bigquery.SchemaField("locRef", "STRING", mode="REQUIRED"), bigquery.SchemaField("busDt", "STRING", mode="REQUIRED")])
                table = bigquery_client.create_table(table)
                print(f"TABLE CREATED dev01-insights.DEV_LND.HOURLY_MICROSPOS_{apiName}_{current_datetime}")
                time.sleep(45)
            
            try:
                errors = bigquery_client.insert_rows_json(f"dev01-insights.DEV_LND.HOURLY_MICROSPOS_{apiName}_{current_datetime}", rows_to_insert);
                print(errors);
            except Exception as e:
                print(f"An error has occurred: {e})") 
                print(f"not able to insert {apiName}")
    except Exception as e:
        print(f"An error has occurred {e} after successful stores: {successful_stores}")
        if successful_stores == 0: # if the API wasn't able to hit any successful stores, then log an error
            if apiName == "getGuestChecks":
                traceback.print_exc()

def getToken(api):
    global token

    cnc_string = get_secret_data(project_id_int, "CRAVEABLE-MICROSPOS-CNC")

    cnc_json = json.loads(cnc_string)

    client_id = cnc_json['CLIENT_ID']
    password = cnc_json['CLIENT_PWD']

    #Authorize
    rsp = session.get(f"{apiAuthEndPoint}/oidc-provider/v1/oauth2/authorize?client_id={client_id}&response_type=code&scope=openid&state=999&redirect_uri=apiaccount://callback&code_challenge=bfYBf5ZqgljYIxuqNrOFxThsVtrVtPi2PGhN3sphKTA&code_challenge_method=S256");

    #Sign In
    form_data = {
        "username": username,
        "password": password,
        "orgname": org,
        "client_id": client_id
    }
    response = session.post(f"{apiAuthEndPoint}/oidc-provider/v1/oauth2/signin", data=form_data);
    rsp = response.json();
    #print(rsp)

    #print(rsp['redirectUrl'].split("?")[1].split("&")[0].replace("code=", ""));

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
    # print(rsp.json());
    token = rsp.json()['access_token']
    getLocations(api);

def getLocations(api):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getLocationDimensions", headers=headers, json=form_data)
    allLocations = rsp.json()['locations']
    locationCount = len(rsp.json()['locations'])
    print(f"API:{api} Total Locations: {locationCount}")

    with ThreadPoolExecutor() as executor:  # parallel processing 20-02-2024 by N.T
        executor.map(getHSTG, [(location, api) for location in allLocations])  # parallel processing 20-02-2024 by N.T

def getGuestChecks(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }

    # GETTING THE UTC TIME TO RUN DELTA PULL FROM GETGUESTCHECKS API
    locationLastTransactionUTC = bigquery_client.query(f"""
        SELECT 
            locRef
            ,STRING(MAX(DATETIME(lastUpdatedUTC))) MAX_LASTUPDATEDUTC
            ,STRING(MAX(DATETIME_SUB(DATETIME(lastUpdatedUTC), INTERVAL 2 HOUR))) MAX_LASTUPDATEDUTC_MINUS_HOUR
        FROM `dev01-insights.DEV_STG.VW_HSTG_MICROSPOS_GETGUESTCHECKS_LAST_35_DAYS_BUSDT` GGC
        WHERE locRef = '{location['locRef']}'
        GROUP BY 1
    """).result();

    maxUpdatedUTC = "2000-01-01T00:00:00" # DEFAULT VALUE IF THERE'S NO MAX LOAD

    for locationUTC in locationLastTransactionUTC: # IF THE LOCATION MATCHES THE CURRENT LOAD, THE PUSH THE DELTA LOAD VALUE
        print(f"locationUTC: {locationUTC}")
        if locationUTC.locRef == location["locRef"]: 
            maxUpdatedUTC = locationUTC.MAX_LASTUPDATEDUTC_MINUS_HOUR.replace(" ", "T")
            print(f"maxUpdatedUTC: {maxUpdatedUTC}")

    form_data = {
        "locRef": location['locRef'], # WILL ALSO NEED TO CHANGE ANYTHING THAT LOOKS LIKE THIS - related to line 139
        "opnBusDt": data_date,
        "changedSinceUTC": maxUpdatedUTC
    }


    request_start_time = time.time()
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getGuestChecks", headers=headers, json=form_data)
    request_end_time = time.time()

    request_time = (f"This getGuestChecks {location['locRef']} request ran for: {request_end_time - request_start_time}") ## CHECKING REQUEST DURATION
    print(request_time)
    
    print(f"python function: def getGuestChecks | API Response Code: {rsp.status_code} | Location: {location['locRef']} | Data Date: {data_date}");
    if rsp.status_code != 504:
        WriteToBigQuery('getGuestChecks', 'guestChecks', rsp, form_data, location['locRef'], data_date)
    elif rsp.status_code == 504:
        print(f"An error has occurred before WriteToBigQuery getGuestChecks Location: {location['locRef']} Data Date: {data_date} Response Code: {rsp.status_code}")
    
def getDiscountDimensions(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getDiscountDimensions", headers=headers, json=form_data)
    WriteToBigQuery('getDiscountDimensions', 'discounts', rsp, form_data, location['locRef'], data_date)

def getNonSalesTransactions(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date,
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getNonSalesTransactions", headers=headers, json=form_data)
    WriteToBigQuery('getNonSalesTransactions', 'nonSalesTransactions', rsp, form_data, location['locRef'], data_date)

def getGuestCheckLineItemExtDetails(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date,
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getGuestCheckLineItemExtDetails", headers=headers, json=form_data)
    WriteToBigQuery('getGuestCheckLineItemExtDetails', 'revenueCenters', rsp, form_data, location['locRef'], data_date)

def getMenuItemPrices(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getMenuItemPrices", headers=headers, json=form_data)
    WriteToBigQuery('getMenuItemPrices', 'menuItemPrices', rsp, form_data, location['locRef'], data_date)

def getServiceChargeDimensions(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getServiceChargeDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getServiceChargeDimensions', 'serviceCharges', rsp, form_data, location['locRef'], data_date)

def getServiceChargeDailyTotals(location, data_date):
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
    WriteToBigQuery('getServiceChargeDailyTotals', 'revenueCenters', rsp, form_data, location['locRef'], data_date)

def getTaxDailyTotals(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getTaxDailyTotals", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getTaxDailyTotals', 'revenueCenters', rsp, form_data, location['locRef'], data_date)

def getOrderTypeDailyTotals(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getOrderTypeDailyTotals", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getOrderTypeDailyTotals', 'revenueCenters', rsp, form_data, location['locRef'], data_date)

def getOrderTypeDimensions(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef']
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getOrderTypeDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getOrderTypeDimensions', 'orderTypes', rsp, form_data, location['locRef'], data_date)

def getEmployeeDimensions(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef']
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getEmployeeDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getEmployeeDimensions', 'employees', rsp, form_data, location['locRef'], data_date)

def getReasonCodeDimensions(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef']
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getReasonCodeDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getReasonCodeDimensions', 'reasonCodes', rsp, form_data, location['locRef'], data_date)

def getCashierDimensions(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "include":"cashiers.num,cashiers.name,cashiers.mstrNum,cashiers.mstrName"
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getCashierDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getCashierDimensions', 'cashiers', rsp, form_data, location['locRef'], data_date)

def getTaxDimensions(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getTaxDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getTaxDimensions', 'taxes', rsp, form_data, location['locRef'], data_date)

def getTenderMediaDimensions(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getTenderMediaDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getTenderMediaDimensions', 'tenderMedias', rsp, form_data, location['locRef'], data_date)

def getMenuItemDimensions(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getMenuItemDimensions", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getMenuItemDimensions', 'menuItems', rsp, form_data, location['locRef'], data_date)

def getControlDailyTotals(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "locRef": location['locRef'],
        "busDt": data_date
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getControlDailyTotals", headers=headers, json=form_data)
    #print(rsp.json())
    WriteToBigQuery('getControlDailyTotals', 'revenueCenters', rsp, form_data, location['locRef'], data_date)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

#def getHSTG(location, apiName):  commented by N.T
def getHSTG(location_api_name_tuple):  # added new function to pass Location and API as tuple
    location, apiName = location_api_name_tuple
    
    print(f"def getHSTG - Location: {location['locRef']} | name: {location['name']} | TimeZone: {location['tz']}")
    local_tz = location['tz']
    local_tz_info = pytz.timezone(local_tz)

    single_date = datetime.now(local_tz_info);
    print(f"Unadjusted Date: {single_date}")
    if 0 <= datetime.now(local_tz_info).hour < 4:
        single_date = (datetime.now(local_tz_info) - timedelta(days=1));
        print(f"Adjusted Date: {single_date}")
    
    if apiName == "getGuestChecks":
        getGuestChecks(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getNonSalesTransactions":
        getNonSalesTransactions(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getGuestCheckLineItemExtDetails":
        getGuestCheckLineItemExtDetails(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getMenuItemPrices":
        getMenuItemPrices(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getDiscountDimensions":
        getDiscountDimensions(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getServiceChargeDimensions":
        getServiceChargeDimensions(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getServiceChargeDailyTotals":
        getServiceChargeDailyTotals(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getTaxDailyTotals":
        getTaxDailyTotals(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getOrderTypeDailyTotals":
        getOrderTypeDailyTotals(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getOrderTypeDimensions":
        getOrderTypeDimensions(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getEmployeeDimensions":
        getEmployeeDimensions(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getReasonCodeDimensions":
        getReasonCodeDimensions(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getCashierDimensions":
        getCashierDimensions(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getTaxDimensions":
        getTaxDimensions(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getTenderMediaDimensions":
        getTenderMediaDimensions(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getMenuItemDimensions":
        getMenuItemDimensions(location, single_date.strftime("%Y-%m-%d"));
    if apiName == "getOrderTypeDailyTotals90":
        getOrderTypeDailyTotals(location, (date.today() + timedelta(days=-90)).strftime("%Y-%m-%d"));

def main(request):
 
    #RE-COMMENT BELOW WHEN DEPLOYING - FOR TESTING
    api_name = request.form.get('api')
    
    print("calling api method: " + api_name)
    
    # RE-COMMENT BELOW WHEN DEPLOYING - FOR TESTING
    getToken(api_name);
    return "done."

if __name__ == "__main__":
    main("")


#main("");