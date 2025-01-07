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
            if rsp.status_code == 200: # if the request to a store is successful for the getGuestChecks API, then iterate counter
                successful_stores += 1
        print(f'Received data calling api {apiName} for {locName} of {busDt}, Total Rows: {len(rsp.json()[arrayName])}')
        if len(rsp.json()[arrayName]) > 0:
            for survey in rsp.json()[arrayName]:
                rows_to_insert.append({"payload": json.dumps(survey), "locRef": locName, "busDt": busDt});
            try:
                bigquery_client.get_table(f"dev01-insights.DEV_LND.BACKFILL_MICROSPOS_{apiName}_{current_date}")
            except NotFound:
                print(f'table not found for calling api {apiName} for {locName} of {busDt}, Total Rows: {len(rsp.json()[arrayName])}')
                table = bigquery.Table(f"dev01-insights.DEV_LND.BACKFILL_MICROSPOS_{apiName}_{current_date}", schema=[bigquery.SchemaField("payload", "STRING", mode="REQUIRED"), bigquery.SchemaField("locRef", "STRING", mode="REQUIRED"), bigquery.SchemaField("busDt", "STRING", mode="REQUIRED")])
                table = bigquery_client.create_table(table)
                time.sleep(45)
            
            try:
                errors = bigquery_client.insert_rows_json(f"dev01-insights.DEV_LND.BACKFILL_MICROSPOS_{apiName}_{current_date}", rows_to_insert);
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
    getTransactionDiscrepancies()

def getLocations(api):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getLocationDimensions", headers=headers, json=form_data)
    allLocations = rsp.json()['locations']

    
    filtered_locations = [location for location in allLocations if re.match("^OP....|^OF....|^CT....|^RR....", location['locRef'])] # N.T test
    with ThreadPoolExecutor() as executor:  # parallel processing 04-03-2024 by N.T     
        executor.map(getHSTG, [(location, api) for location in filtered_locations])

def getGuestChecks(location, data_date):
    headers = {
        "Authorization": "Bearer " + token
    }


    form_data = {
        "locRef": location,
        "busDt": data_date,
    }
    rsp = session.post(f"{apiEndPoint}/bi/v1/{org}/getGuestChecks", headers=headers, json=form_data)
    print(f"python function: def getGuestChecks | API Response Code: {rsp.status_code} | Location: {location} | Data Date: {data_date}");
    WriteToBigQuery('getGuestChecks', 'guestChecks', rsp, form_data, location, data_date)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def getTransactionDiscrepancies():
    #now get getGuestChecks for each location and difference record
    # discrepancies = bigquery_client.query("SELECT * FROM prod-insights.QA.VW_QA_MICROSPOS_07_DAY_BACKFILL_DISCREPANCIES").result();
    discrepancies = bigquery_client.query("SELECT * FROM prod-insights.QA.VW_QA_MICROSPOS_07_DAY_BACKFILL_DISCREPANCIES_UPDATED").result();
    discrepancies_count = bigquery_client.query("SELECT COUNT(*) AS DISCREPANCY_COUNT FROM prod-insights.QA.VW_QA_MICROSPOS_07_DAY_BACKFILL_DISCREPANCIES_UPDATED").result();
    
    for discrepancy in discrepancies_count:
        print(f"Total rows of Discrepancies: {discrepancy.DISCREPANCY_COUNT}") 

    for discrep in discrepancies:
        # print(discrep)
        print(f"Discrepancies: locRef: {discrep.locRef} busDt: {discrep.busDt}")
        locRef = discrep.locRef;
        busDt = discrep.busDt;
        match = re.search("^OP....|^OF....|^CT....|^RR....", locRef)
        if match:
            #if locRef == "OP2348":
            getGuestChecks(locRef, busDt);
        if not match:
            print(f"Test store skipped: {locRef}")

# @functions_framework.http

def main(request):
    #BELOW LINE NEEDS TO BE RECOMMENTED WHEN PUSHED
    api_name = request.form.get('api')
    
    # print("calling api method: " + api_name)
    getToken(api_name);
    return "done."

if __name__ == "__main__":
    main("")
