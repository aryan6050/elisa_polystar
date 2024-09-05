import json
import requests
import pandas as pd
import logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

log_path = os.path.join(parent_dir, 'app.log')

logging.basicConfig(filename=log_path, level=logging.INFO)

def delete_index_pattern(config):
    try:
        file_path = os.path.join(parent_dir, 'delete.xlsx')
        excel_data = pd.read_excel(file_path)
        es_host = config["elasticsearch_attributes"]["host"]
        es_port = config["elasticsearch_attributes"]["port"]
        es_scheme = config["elasticsearch_attributes"]["scheme"]
        username = config["elasticsearch_attributes"]["username"]
        password = config["elasticsearch_attributes"]["password"] 

        columns_of_interest = ['index_pattern']

        for index, row in excel_data.iterrows():
            name = row['index_pattern']
            # Send the request to Elasticsearch to create the index pattern
            url_pattern = f"{es_scheme}://{es_host}:{es_port}/.kibana/_doc/index-pattern:{name}"
            headers = {'Content-Type': 'application/json'}
            response_pattern = requests.delete(url_pattern, auth=(username, password), headers=headers)
                
            if response_pattern.status_code == 200:
                logging.info(f"Index pattern {name} deleted successfully!")
            elif response_pattern.status_code == 404:
                # Index pattern not found, log a message
                logging.info(f"Index pattern {name} not found. Skipping deletion.")
            else:
                logging.error(f"Failed to delete index pattern {name}. Status code: {response_pattern.status_code}")
                logging.error(f"Response: {response_pattern.text}")

    except Exception as e:
        logging.error(f"Error: {e}")
