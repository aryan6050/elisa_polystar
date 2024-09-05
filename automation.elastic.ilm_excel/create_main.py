import yaml
from create.create_policy import *
from create.create_template import *
from create.create_index import *
from create.schedule_query import *
from create.create_index_pattern import *
from create_token import *
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to config.yaml
config_path = os.path.join(script_dir, 'config.yaml')

if __name__ == '__main__':
    with open(config_path) as config_file:
        config = yaml.load(config_file, Loader=yaml.SafeLoader)
    # Call the functions to create ILM policy and template
    create_ilm_policy(config)
    create_index_template(config)
    create_index(config)
    token = get_token(config)
    run_query(config, token)
    create_index_pattern(config)
    