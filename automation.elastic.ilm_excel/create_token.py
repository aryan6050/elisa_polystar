import json
import requests

def get_token(config):

    payload = {
            "username": config["use_case_engine_attributes"]["username"],
            "password": config["use_case_engine_attributes"]["password"]
        }

    uri = config["use_case_engine_attributes"]["uri"]
        
    response = requests.post(uri, json=payload, verify = False)
    token = response.json()['token']
    return token