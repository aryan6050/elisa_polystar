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


def get_all_rollover_indices(config):

    es_host = config["elasticsearch_attributes"]["host"]
    es_port = config["elasticsearch_attributes"]["port"]
    es_scheme = config["elasticsearch_attributes"]["scheme"]
    username = config["elasticsearch_attributes"]["username"]
    password = config["elasticsearch_attributes"]["password"]

    # Send the request to Elasticsearch to get all indices
    url = f"{es_scheme}://{es_host}:{es_port}/_aliases"  # Change format to JSON
    response = requests.get(url, auth=(username, password))

    if response.status_code != 200:
        logging.error(f"Failed to retrieve aliases: {response.text}")
        return []

    aliases_info = response.json()
    rollover_indices = []

    # Iterate over the aliases to find rollover indices
    for index, index_info in aliases_info.items():
        for alias, alias_info in index_info['aliases'].items():
            if (not alias.startswith('.')) and (alias_info.get('is_write_index') == False):
                rollover_indices.append(index)

    logging.info(f"retrieve aliases: {rollover_indices}")

    return rollover_indices



def get_empty_rollover_indices(config, rollover_indices):
    try:
        es_host = config["elasticsearch_attributes"]["host"]
        es_port = config["elasticsearch_attributes"]["port"]
        es_scheme = config["elasticsearch_attributes"]["scheme"]
        username = config["elasticsearch_attributes"]["username"]
        password = config["elasticsearch_attributes"]["password"]
            
        empty_rollover_indices = []
        for index in rollover_indices:
            # Check index stats to see if it has documents
            stats_url = f"{es_scheme}://{es_host}:{es_port}/{index}/_stats/docs?pretty"
            stats_response = requests.get(stats_url, auth=(username, password))
                
            if stats_response.status_code == 200:
                stats_info = stats_response.json()
                doc_count = stats_info['indices'][index]['total']['docs']['count']
                    
                if doc_count == 0:
                    empty_rollover_indices.append(index)
            else:
                logging.error(f"Failed to retrieve stats for index {index}. Status code: {stats_response.status_code}")
                logging.error(f"Response content: {stats_response.content.decode()}")
            
        logging.info(f"Empty rollover indices to be deleted are : {empty_rollover_indices}")

        return empty_rollover_indices

    except Exception as e:
        logging.error(f"Error: {e}")
    

def delete_index(config):
    try:
        es_host = config["elasticsearch_attributes"]["host"]
        es_port = config["elasticsearch_attributes"]["port"]
        es_scheme = config["elasticsearch_attributes"]["scheme"]
        username = config["elasticsearch_attributes"]["username"]
        password = config["elasticsearch_attributes"]["password"]

        rollover_indices = get_all_rollover_indices(config)

        # Send the request to Elasticsearch to create the index
        url = f"{es_scheme}://{es_host}:{es_port}"
        headers = {'Content-Type': 'application/json'}

        index_list = get_empty_rollover_indices(config, rollover_indices)

        logging.info(f"Empty rollover indices to be deleted are part 2 is  : {index_list}")

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

# Example usage
if __name__ == "__main__":
    # Load config from a YAML or JSON file
    with open(os.path.join(parent_dir, 'config.yaml'), 'r') as config_file:
        config = yaml.safe_load(config_file)
     
    # indices  = get_all_rollover_indices(config)
    # get_empty_rollover_indices(config,indices)
    delete_index(config)
    
