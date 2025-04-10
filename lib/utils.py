import os
import datetime
import logging
import copy
import sys
import random
import string

from logging.config import fileConfig
from multiprocessing import Process, Manager

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

######################## MEMORY

# Read a file line by line without loading the entire file into memory
def read_file_line_by_line(filepath):
    with open(filepath, "r") as fr:
        for line in fr:
            yield line.strip()
            
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

######################## ASYNC

class FileWriter(object):

    __queue = Manager().Queue()

    def __init__(self, mode="a"):
        self.__mode = mode
        self.__process = Process(target=self.__process_queue)
        self.__process.start()

    def __process_queue(self):

        file_handler = {}

        while True:
            filepath, line = self.__queue.get()
            
            if filepath is None and line is None: # Shutdown
                break
            
            if filepath not in file_handler:
                file_handler[filepath] = open(filepath, self.__mode)

            file_handler[filepath].write("{}\n".format(line))
            file_handler[filepath].flush()

        for fw in file_handler.values():
            fw.close()

    def shutdown(self):
        self.__queue.put((None, None))
        self.__process.join()

    @classmethod
    def write(cls, filepath, line):
        cls.__queue.put((filepath, line))
            
######################## PROGRAM ENTITIES

# Return a list of tuples (name, class) of all subclasses for a Class given
def get_subclasses(parent_class):
    return parent_class.__subclasses__()

# Return a list of tuples (name, function) of a Class given
# Can be added some filterings for the functions
def get_functions(class_type, filters=[
    lambda name, function : not name.startswith("_"), # not protected functions
    lambda name, function : not name.startswith("__"), # not private functions
    lambda name, function : not isinstance(function, classmethod), # not classmethods
    lambda name, function : not isinstance(function, staticmethod) # not staticmethods
]):
    fixed_filters = [
        lambda name, function : callable(getattr(class_type, name)) # Can be called as function
    ]

    return [
        (name, function)
        for name, function
        in class_type.__dict__.items()
        if all(
            check(name, function)
            for check
            in filters + fixed_filters
        )
    ]
            
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

######################## CLASS

# A wrapper class to store and apply a picklable function.
class MakeItPicklableWrapper:
    def __init__(self, key_func):
        self.__args = []
        self.__kwargs = {}
        self.__key_func = key_func  # Store the function

    def add_args(self, *args):
        self.__args = args
        return self

    def add_kwargs(self, **kwargs):
        self.__kwargs = kwargs
        return self

    def __call__(self, data):
        return self.__key_func(data, *self.__args, **self.__kwargs)  # Make it callable

class UtilityFunction:
    
    @staticmethod
    def predicate(function_name):
        callable_function = next((
            function
            for name, function
            in get_functions(UtilityFunction.PredicateFunction)
            if name == function_name
        ), None)

        if not callable_function:
            raise NotImplementedError("{} function is not implemented".format(function_name))

        return callable_function

    @staticmethod
    def generate_value(function_name):
        callable_function = next((
            function
            for name, function
            in get_functions(UtilityFunction.GeneratorFunction)
            if name == function_name
        ), None)

        if not callable_function:
            raise NotImplementedError("{} function is not implemented".format(function_name))

        return callable_function

    class PredicateFunction:

        def equals(x, y):
            return x == y

        def greater_than(x, y):
            return x > y

        def greater_equal(x, y):
            return x >= y

        def less_than(x, y):
            return x < y

        def less_equal(x, y):
            return x <= y

        def not_equals(x, y):
            return x != y

    class GeneratorFunction:

        def incremental_1(value):
            return str(int(value) + 1).rjust(len(value), '0')

        def incremental_2(value):
            return str(int(value) + 2).rjust(len(value), '0')

        def incremental_3(value):
            return str(int(value) + 3).rjust(len(value), '0')

        def timeYYYYMMDDHH24MISS(value):
            return get_time(dateformat="%Y%m%d%H%M%S")

        def timeYYMMDDHH24MISS(value):
            return get_time(dateformat="%y%m%d%H%M%S")

        def timeYYYYMMDD(value):
            return get_time(dateformat="%Y%m%d")

        def timeYYMMDD(value):
            return get_time(dateformat="%y%m%d")

        def timeHH24MISS(value):
            return get_time(dateformat="%H%M%S")

        def timeHH24_MI_SS(value):
            return get_time(dateformat="%H:%M:%S")

        def random_char_29(value):
            return ''.join(random.choice(string.ascii_lowercase) for i in range(29))

        def random_digit_12(value):
            return ''.join(random.choice(string.digits) for i in range(12))

        def random_digit_1(value):
            return ''.join(random.choice(string.digits) for i in range(1))

        def random_digit_2(value):
            return ''.join(random.choice(string.digits) for i in range(2))

        def random_digit_3(value):
            return ''.join(random.choice(string.digits) for i in range(3))

        def random_digit_5(value):
            return ''.join(random.choice(string.digits) for i in range(5))

        def random_digit_2_without_0(value):
            return str(random.randint(1, 99))

        def fill_random_char_29(value):
            return value + "".join(random.choice(string.ascii_lowercase) for i in range(29 - len(value)))

        def keep_3_first_fill_random_char_46(value):
            return value[0:3] + "".join(random.choice(string.ascii_lowercase) for i in range(46 - 3))

        def incremental_random_char_29(value):
            incremental_length = 9
            if value is None or value == "":
                return "0".zfill(incremental_length) + "".join(random.choice(string.ascii_lowercase) for i in range(29 - incremental_length))
            else:
                return str(int(value[0:incremental_length]) + 1).zfill(incremental_length) + "".join(random.choice(string.ascii_lowercase) for i in range(29 - incremental_length))

        def UTC_offset_XXX(value):
            # format X00
            is_dst = time.daylight and time.localtime().tm_isdst > 0
            utc_offset = -(time.altzone if is_dst else time.timezone) / 60 / 60
            return str(int(utc_offset) * 100)

        def UTC_offset__XXXX(value):
            # format X00
            is_dst = time.daylight and time.localtime().tm_isdst > 0
            utc_offset = -(time.altzone if is_dst else time.timezone) / 60 / 60
            return "+" if utc_offset > 0 else "-" + str(int(utc_offset) * 100).rjust(4, '0')

        #####################
        # csv_aggregator.py #
        #####################

        # sum of the depth of the array
        def csv_aggregator_sum(array_list):
            total = 0
            for item in array_list:
                if isinstance(item, list):
                    total += UtilityFunction.GeneratorFunction.csv_aggregator_sum(item)
                elif isinstance(item, int):
                    total += item
                else:
                    total += int(item)
            return total

        def csv_aggregator_count(array_list):
            return len(array_list)

        ####################
        # csv_converter.py #
        ####################
        
        def csv_converter_row_replace(array_list, check_column, check_value, replace_column, replace_value, is_header=False):
            if array_list[check_column] == check_value:
                array_list[replace_column] = replace_value
            return array_list

        def csv_converter_digit_sum(array_list, pos_res, pos_sum_x, pos_sum_y, is_header=False):
            if is_header:
                return array_list
            array_list[pos_res] = int(array_list[pos_sum_x]) + int(array_list[pos_sum_y])
            return [ str(item) for item in array_list ]

        def csv_converter_append_field(array_list, field_name, default_value, is_header=False):
            if is_header:
                default_value = field_name
            return array_list + [default_value] 

        def csv_converter_change_field_name(array_list, position, field_name, is_header=False)
            if is_header:
                array_list[position] = field_name
            return array_list

        def csv_converter_string_sum(array_list, pos_res, pos_sum_x, pos_sum_y, is_header=False):
            if is_header:
                return array_list
            array_list[pos_res] = str(array_list[pos_sum_x]) + str(array_list[pos_sum_y])
            return array_list
