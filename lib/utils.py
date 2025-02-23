import os
import datetime
import logging

from logging.config import fileConfig

######################## DATES

# Given a date convert into string formatted
def get_time(date=datetime.datetime.now(), dateformat="%Y-%m-%d %H:%M:%S"):
    return date.strftime(dateformat)

######################## FOLDERS

# IMPROVEMENT: Can rewrite the function to accept recursion create paths
def create_if_not_exists_folders(folders):
    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder)
            
######################## JSON

# Create some exceptions when reading a JSON file
# not_check: apply some functions to skip some keys for the duplicate validation
def json_raise_on_duplicates(key_value_pairs, not_check=[]):
    # Reject duplicate keys
    registered_keys = {}
    
    for key, value in key_value_pairs:
        to_be_filtered = next((True for function in not_check if function(key)), False)
        if not to_be_filtered:
            if key in registered_keys:
                raise ValueError("Duplicate key: {}".format(key))
            else:
                registered_keys[key] = value
    
    return registered_keys
                