import json
import requests
import pandas as pd
import yaml
import logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

log_path = os.path.join(parent_dir, 'app.log')

logging.basicConfig(filename=log_path, level=logging.INFO)

def delete_index_template(config):
    try:
        file_path = os.path.join(parent_dir, 'delete.xlsx')
        excel_data = pd.read_excel(file_path)
        es_host = config["elasticsearch_attributes"]["host"]
        es_port = config["elasticsearch_attributes"]["port"]
        es_scheme = config["elasticsearch_attributes"]["scheme"]
        username = config["elasticsearch_attributes"]["username"]
        password = config["elasticsearch_attributes"]["password"]

        for index_template_name in excel_data['template_name']:
            url = f"{es_scheme}://{es_host}:{es_port}/_index_template/{index_template_name}"
            headers = {'Content-Type': 'application/json'}
            response = requests.delete(url, auth=(username, password), headers=headers)

            if response.status_code == 200:
                logging.info(f"Index template {index_template_name} deleted successfully")
            else:
                logging.error(f"Failed to delete index template {index_template_name}. Status code: {response.status_code}")
                logging.error(f"Response: {response.text}")

    except Exception as e:
        logging.error(f"Error: {e}")
