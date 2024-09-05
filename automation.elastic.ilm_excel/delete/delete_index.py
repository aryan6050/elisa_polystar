import json
import requests
from delete.get_indices import *
import logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

log_path = os.path.join(parent_dir, 'app.log')

logging.basicConfig(filename=log_path, level=logging.INFO)

def delete_index(config):
    try:
        file_path = os.path.join(parent_dir, 'delete.xlsx')
        excel_data = pd.read_excel(file_path)
        es_host = config["elasticsearch_attributes"]["host"]
        es_port = config["elasticsearch_attributes"]["port"]
        es_scheme = config["elasticsearch_attributes"]["scheme"]
        username = config["elasticsearch_attributes"]["username"]
        password = config["elasticsearch_attributes"]["password"]

        # Send the request to Elasticsearch to create the index
        url = f"{es_scheme}://{es_host}:{es_port}"
        headers = {'Content-Type': 'application/json'}
        for rollover_alias in excel_data['rollover_alias']:
            index_list = get_indices(config, rollover_alias)
            for index in index_list:
                logging.info(f"the index to be deleted is {index}")
                try:
                    response = requests.delete(f"{url}/{index}", auth=(username, password), headers=headers)
                    if response.status_code == 200:
                        logging.info(f"Index '{index}' deleted successfully")
                    else:
                        logging.error(f"Failed to delete index '{index}'. Status code: {response.status_code}")
                        logging.error(f"Response: {response.text}")
                except Exception as e:
                    logging.error(f"Error deleting index '{index}': {e}")

    except Exception as e:
        logging.error(f"Error: {e}")
