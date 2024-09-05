import requests
import pandas as pd
import logging
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)


log_path = os.path.join(parent_dir, 'app.log')

logging.basicConfig(filename=log_path, level=logging.INFO)

def create_ilm_policy(config):
    try:
        file_path = os.path.join(parent_dir, 'create.xlsx')
        excel_data = pd.read_excel(file_path)
        es_host = config["elasticsearch_attributes"]["host"]
        es_port = config["elasticsearch_attributes"]["port"]
        es_scheme = config["elasticsearch_attributes"]["scheme"]
        username = config["elasticsearch_attributes"]["username"]
        password = config["elasticsearch_attributes"]["password"]

        columns_of_interest = ['policy_name', 'policy_hot_max_age', 'policy_hot_max_size', 'policy_warm_min_age', 'policy_cold_min_age', 'policy_delete_min_age']

        # Iterate over rows and fetch values from specified columns
        for index, row in excel_data.iterrows():
            name = row['policy_name']
            policy_hot_max_age = row['policy_hot_max_age']
            policy_hot_max_size = row['policy_hot_max_size']
            policy_warm_min_age = row['policy_warm_min_age']
            policy_cold_min_age = row['policy_cold_min_age']
            policy_delete_min_age = row['policy_delete_min_age']

            ilm_policy = {
                "policy": {
                    "phases": {
                        "hot": {
                            "actions": {
                                "rollover": {
                                    "max_age": policy_hot_max_age,
                                    "max_size": policy_hot_max_size
                                }
                            }
                        },
                        "warm": {
                            "min_age": policy_warm_min_age,
                            "actions": {
                                "allocate": {
                                    "require": {
                                        "box_type": "warm"
                                    }
                                }
                            }
                        },
                        "cold": {
                            "min_age": policy_cold_min_age,
                            "actions": {
                                "allocate": {
                                    "require": {
                                        "box_type": "cold"
                                    }
                                }
                            }
                        },
                        "delete": {
                            "min_age": policy_delete_min_age,
                            "actions": {
                                "delete": {}
                            }
                        }
                    }
                }
            }

            # Send the request to Elasticsearch to create the ILM policy
            url = f"{es_scheme}://{es_host}:{es_port}/_ilm/policy/{name}"
            headers = {'Content-Type': 'application/json'}
            response = requests.put(url, auth=(username, password), headers=headers, json=ilm_policy)

            if response.status_code == 200:
                logging.info(f"ILM policy created {name} successfully")
            else:
                logging.error(f"Failed to create ILM policy {name}. Status code: {response.status_code}")
                logging.error(f"Response: {response.text}")

    except Exception as e:
        logging.error(f"Error: {e}")
