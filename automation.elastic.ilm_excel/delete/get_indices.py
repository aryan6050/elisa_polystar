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

def get_indices(config, rollover_alias):
    try:
        file_path = os.path.join(parent_dir, 'delete.xlsx')
        excel_data = pd.read_excel(file_path)
        es_host = config["elasticsearch_attributes"]["host"]
        es_port = config["elasticsearch_attributes"]["port"]
        es_scheme = config["elasticsearch_attributes"]["scheme"]
        username = config["elasticsearch_attributes"]["username"]
        password = config["elasticsearch_attributes"]["password"]

        # Send the request to Elasticsearch to get all indices
        url = f"{es_scheme}://{es_host}:{es_port}/_cat/indices?v&format=json"  # Change format to JSON
        response = requests.get(url, auth=(username, password))

        if response.status_code == 200:
            indices_info = response.json() 
            index_list = []
            for index in indices_info:
                if index['index'].startswith(f'{rollover_alias}-0'):
                    index_list.append(index['index'])
            logging.info(f"printing index list before returning {index_list}")
            return index_list
        else:
            logging.error(f"Failed to retrieve indices {rollover_alias}. Status code: {response.status_code}")
            logging.error(f"Response content: {response.content.decode()}")

    except Exception as e:
        logging.error(f"Error: {e}")
