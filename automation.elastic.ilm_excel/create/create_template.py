import requests
import pandas as pd
import logging
import json
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

log_path = os.path.join(parent_dir, 'app.log')

logging.basicConfig(filename=log_path, level=logging.INFO)

def create_index_template(config):
    try:
        file_path = os.path.join(parent_dir, 'create.xlsx')
        excel_data = pd.read_excel(file_path)
        es_host = config["elasticsearch_attributes"]["host"]
        es_port = config["elasticsearch_attributes"]["port"]
        es_scheme = config["elasticsearch_attributes"]["scheme"]
        username = config["elasticsearch_attributes"]["username"]
        password = config["elasticsearch_attributes"]["password"]

        columns_of_interest = ['index_pattern','template_priority','no_of_shards','no_of_replicas','policy_name','rollover_alias','template_dynamic_mapping']

        # Iterate over rows and fetch values from specified columns
        for index, row in excel_data.iterrows():
            name = row['template_name']
            index_pattern = row['index_pattern']
            priority = row['template_priority']
            no_of_shards = row['no_of_shards']
            no_of_replicas = row['no_of_replicas']
            policy_name = row['policy_name']
            rollover_alias = row['rollover_alias']
            dynamic_mapping = json.loads(row['template_dynamic_mapping'])

            dynamic_templates = []
            for key,value in dynamic_mapping.items():
                mapping_json = {
                        (f"{key[:-2]}_as_{value}"): {
                            "path_match": key,
                            "mapping": {
                                "type": value
                            }
                       }
                    }
                dynamic_templates.append(mapping_json)

            properties = edit_template_mapping(name)
            print("properties is ",properties)
            # Define the index template
            index_template = {
                "index_patterns": index_pattern,
                "priority": priority,
                "template": {
                    "settings": {
                    "number_of_shards": no_of_shards,
                    "number_of_replicas": no_of_replicas,
                    "index.lifecycle.name": policy_name,
                    "index.lifecycle.rollover_alias": rollover_alias
                },
                    "mappings": {
                        "dynamic_templates": dynamic_templates,
                        "properties": properties
                    }
                }
            }

            print("index template is ",index_template)

            # Send the request to Elasticsearch to create the index template
            url = f"{es_scheme}://{es_host}:{es_port}/_index_template/{name}"
            headers = {'Content-Type': 'application/json'}
            response = requests.put(url, auth=(username, password), headers=headers, json=index_template)

            if response.status_code == 200:
                logging.info(f"Index template {name} created successfully")
            else:
                logging.error(f"Failed to create index template {name}. Status code: {response.status_code}")
                logging.error(f"Response: {response.text}")

    except Exception as e:
        logging.error(f"Error: {e}")


def edit_template_mapping(template_name):
    try:
        file_path = os.path.join(parent_dir, 'edit_template.xlsx')
        excel_data = pd.read_excel(file_path, sheet_name=None)

        data_dict = {}
        properties_dict = {}


        for sheet_name, sheet_data in excel_data.items():
            if sheet_name == template_name:
                # Extract values from two columns and store in dictionary
                data_dict = sheet_data[['field_name', 'datatype']].to_dict(orient='records')
                print(data_dict)
                for data in data_dict:
                    properties_dict[data['field_name']] = {"type" : "text",
                                                           "fields": {
                                                                        "keyword": {
                                                                        "type": data['datatype'],
                                                                        "ignore_above": 256
                                                                        }

                                                                 }
                                                        }
        
        return properties_dict

    except Exception as e:
        logging.error(f"Error: {e}")