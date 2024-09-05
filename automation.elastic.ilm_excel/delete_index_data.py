import os
import pandas as pd
import yaml
import logging
import time
from elasticsearch import Elasticsearch
from delete.get_indices import get_indices

# Logging configuration
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
log_path = os.path.join(parent_dir, 'app.log')
logging.basicConfig(filename=log_path, level=logging.INFO)


def delete_index_data(config):
    try:
        file_path = os.path.join(script_dir, 'delete.xlsx')
        excel_data = pd.read_excel(file_path)
        es_host = config["elasticsearch_attributes"]["host"]
        es_port = config["elasticsearch_attributes"]["port"]
        username = config["elasticsearch_attributes"]["username"]
        password = config["elasticsearch_attributes"]["password"]

        # Configure Elasticsearch client with retry and timeout settings
        es = Elasticsearch(
            [f"http://{es_host}:{es_port}"],
            basic_auth=(username, password),
            headers={'Content-Type': 'application/json'},
            request_timeout=60000
        )

        for i, row in excel_data.iterrows():
            rollover_alias = row['rollover_alias']
            date_range_gte = row['date_range_gte']
            date_range_lte = row['date_range_lte']
            qh = row['qh']

            # hour = row['hour']
            # hour_str = '{:02d}'.format(hour)
            # print("Formatted hour:", hour_str)
            qh = str(row['qh'])
            # ne_type = row['ne_type']
            index_list = get_indices(config, rollover_alias)
            # query = {
            #     "query": {
            #         "range": {
            #             "date": {
            #                  "gte": (f'{date_range_gte}'),
            #                  "lte": (f'{date_range_lte}')
            #             }
            #         }
            #     }
            # }

            query = {
                    "query": {
                        "bool" : {
                        "must" : [
                            {
                            "term" : {
                                "qh" : {
                                "value" : qh
                                }
                            }
                        },
                            { 
                            "range" : {
                                "date": {
                                    "gte": (f'{date_range_gte}'),
                                    "lte": (f'{date_range_lte}')
                                }
                            }
                        }
                    ]
                    }
                }
            }

            for index in index_list:
                logging.info(f"The index to be deleted is {index}")
                try:
                    response = es.delete_by_query(index=index, body=query, wait_for_completion=True)
                    # time.sleep(10)
                    if response['deleted'] > 0:
                        logging.info(f"Index documents for index '{index}' deleted successfully. Response : {response}")
                    else:
                        logging.info(f"No documents to be deleted within the time range for index {index}. Response: {response}")
                    # print(response)
                    # task_id = response['task']
                    # logging.info(f"The task id for the request is {task_id}.")
                    
                    # #for getting the task information
                    # response = es.tasks.get(task_id)
                    # status = {response['_source']['state']}

                    # while status == "completed":
                    #     time.sleep(30)
                    #     response = es.tasks.get(task_id)
                    #     status = {response['_source']['state']}
                    #     logging.info(f"The status for task is {status}.")
                    
                    # logging.info(f"Index documents for index '{index}' deleted successfully. Response : {response}")
                
                except Exception as e:
                    logging.error(f"Error deleting index documents '{index}': {e}")

    except KeyError as e:
        logging.error(f"Missing configuration key: {e}")

if __name__ == '__main__':
    config_path = os.path.join(script_dir, 'config.yaml')

    try:
        with open(config_path) as config_file:
            config = yaml.safe_load(config_file)

        delete_index_data(config)

    except FileNotFoundError:
        logging.error(f"Config file not found at {config_path}")
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
