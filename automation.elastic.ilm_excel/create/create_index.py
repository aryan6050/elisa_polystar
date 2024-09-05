import requests
import pandas as pd
import logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

log_path = os.path.join(parent_dir, 'app.log')

logging.basicConfig(filename=log_path, level=logging.INFO)


def create_index(config):
    try:
        file_path = log_path = os.path.join(parent_dir, 'create.xlsx')
        excel_data = pd.read_excel(file_path)
        es_host = config["elasticsearch_attributes"]["host"]
        es_port = config["elasticsearch_attributes"]["port"]
        es_scheme = config["elasticsearch_attributes"]["scheme"]
        username = config["elasticsearch_attributes"]["username"]
        password = config["elasticsearch_attributes"]["password"]

        columns_of_interest = ['index','rollover_alias']

        # Iterate over rows and fetch values from specified columns
        for index, row in excel_data.iterrows():
            name = row['index']
            rollover_alias = row['rollover_alias']

            # Define the index settings
            index_settings = {
                "aliases": {
                    rollover_alias: {
                        "is_write_index": True
                    }
                }
            }

            # Send the request to Elasticsearch to create the index
            url = f"{es_scheme}://{es_host}:{es_port}/{name}"
            headers = {'Content-Type': 'application/json'}
            response = requests.put(url, auth=(username, password), headers=headers, json=index_settings)

            if response.status_code == 200:
                logging.info(f"Index {name} created successfully")
            else:
                logging.error(f"Failed to create index {name}. Status code: {response.status_code}")
                logging.error(f"Response: {response.text}")

    except Exception as e:
        logging.error(f"Error: {e}")
