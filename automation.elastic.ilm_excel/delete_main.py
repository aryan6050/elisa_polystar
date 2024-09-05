import yaml
from delete.delete_policy import *
from delete.delete_template import *
from delete.delete_index import *
from delete.delete_index_pattern import *
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to config.yaml
config_path = os.path.join(script_dir, 'config.yaml')


if __name__ == '__main__':
    with open(config_path) as config_file:
        config = yaml.load(config_file, Loader=yaml.SafeLoader)
    # Call the functions to create ILM policy and template
    delete_index_pattern(config)
    delete_index(config)
    delete_index_template(config)
    delete_ilm_policy(config)