import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime
import pandas as pd
import logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

log_path = os.path.join(parent_dir, 'app.log')

logging.basicConfig(filename=log_path, level=logging.INFO)


def run_query(config, token):
    file_path = os.path.join(parent_dir, 'create.xlsx')
    excel_data = pd.read_excel(file_path)
    token_value = token
    private_url = config["use_case_engine_attributes"]["api_uri"]

    columns_of_interest = ['project_id','query_id','query_name']

    # Iterate over rows and fetch values from specified columns
    for index, row in excel_data.iterrows():
        project_id = row['project_id']
        query_id = row['query_id']
        query_name = row['query_name']

        run_query_id = query_id
        run_query_name = query_name
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_query_name = f"{run_query_name}_{timestamp_str}"

        payload = {
            "query": "\nmutation SaveQueryPlan($queryPlan: QueryPlanInput!) {\n  saveQueryPlan(input: $queryPlan) {\n    _id\n  }\n}\n",
            "operationName": "SaveQueryPlan",
            "variables": {
                "queryPlan": {
                    "queriesIds": run_query_id,
                    "cron": None,
                    "name": run_query_name,
                    "config": []
                }
            }
        }

        # Send the POST request
        headers = {
            'Authorization': 'Bearer ' + token_value,
            'Content-Type': 'application/json',
        }

        # Send the POST request with headers
        private_url_response = requests.post(
            url=private_url,
            json=payload,
            headers=headers,verify = False
        )
        data = private_url_response.json()
        logging.info(f"the data for the response is {data}")

        status_code = private_url_response.status_code

        if private_url_response.status_code == 200:
            try:
                logging.info(f"the query {run_query_id} executed successfully.")
                response_data = private_url_response
                logging.info(f"Response data is : {response_data}")
            except json.decoder.JSONDecodeError:
                logging.error("Request failed")
        else:
            logging.error(f"Request failed with status code: {private_url_response.status_code}")

