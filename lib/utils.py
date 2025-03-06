import os
import datetime
import logging
import copy

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
def json_raise_on_duplicates(key_value_pairs, not_check=[], dictionary_type=dict):
    # Reject duplicate keys
    registered_keys = dictionary_type()
    
    for key, value in key_value_pairs:
        to_be_filtered = next((True for function in not_check if function(key)), False)
        if not to_be_filtered:
            if key in registered_keys:
                raise ValueError("Duplicate key: {}".format(key))
            else:
                registered_keys[key] = value
    
    return registered_keys
            
######################## CLASSES

# Return a list of tuples (name, class) of all subclasses for a Class given
def get_subclasses(parent_class):
    return parent_class.__subclasses__()
            
######################## DATA TRANSFORMATION

# For any combination of list + dict, it unpacks the values and return by data depth
# It should ensure all yield values are the same length, otherwise exception is triggered
def flatten(data, values=[]):
    copy_values = copy.deepcopy(values)

    if isinstance(data, dict):
        for key, value in data.items():
            copy_values.append(key)
            yield from flatten(value, copy_values)
    elif isinstance(data, list):
        for value in data:
            yield from flatten(value, copy_values)
    else:
        copy_values.append(str(data))
        yield copy_values
                