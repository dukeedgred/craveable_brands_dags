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
apiEndPoint = "https://qsrh-cnc.oraclemicros.com"
username = "OP CNC TEST"
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

def WriteToBigQuery(apiName, arrayName, rsp, body):
    rows_to_insert = [];
    # global successful_stores
    apiName = apiName.upper()
    print(f"{apiName}: {rsp.status_code}")
    try:
        print(f'Received data calling api {apiName}, Total Rows: {len(rsp.json()[arrayName])}')
        if len(rsp.json()[arrayName]) > 0:
            for survey in rsp.json()[arrayName]:
                rows_to_insert.append({"payload": json.dumps(survey)});
            try:
                bigquery_client.get_table(f"dev01-insights.DEV_LND.MICROSPOS_{apiName}_{current_date}")
            except NotFound:
                table = bigquery.Table(f"dev01-insights.DEV_LND.MICROSPOS_{apiName}_{current_date}", schema=[bigquery.SchemaField("payload", "STRING", mode="REQUIRED")])
                table = bigquery_client.create_table(table)
                time.sleep(45)
            
            try:
                errors = bigquery_client.insert_rows_json(f"dev01-insights.DEV_LND.MICROSPOS_{apiName}_{current_date}", rows_to_insert);
                print(errors);
            except Exception as e:
                print(f"An error has occurred: {e}") 
                print(f"not able to insert {apiName}")
    except Exception as e:
        if apiName == "GETCOMBOMEALS":
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
    print(rsp)

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
    token = rsp.json()['access_token']
    print(token)
    getAPIRequest(api);


def getAPIRequest(apiname):
    headers = {
        "Authorization": "Bearer " + token
    }
    form_data = {
        "offset" : 0
    }
    rsp = session.post(f"{apiEndPoint}/config/sim/v1/menuitems/{apiname}", headers=headers, json=form_data)

    total_records = int(rsp.json()['totalResults'])
    WriteToBigQuery(f'{apiname}', 'items', rsp, form_data)

    for i in range(100,total_records,100):
        form_data = {
        "offset": i,
        }
        
        rsp = session.post(f"{apiEndPoint}/config/sim/v1/menuitems/{apiname}", headers=headers, json=form_data)
        WriteToBigQuery(f'{apiname}', 'items', rsp, form_data)


def main(request):
 
    #RE-COMMENT BELOW WHEN DEPLOYING - FOR TESTING
    api_name = request.form.get("api")
    
    print("calling api method: " + api_name)
    
    getToken(api_name);
    return "done."

# if __name__ == "__main__":
#     main("")


#main("");