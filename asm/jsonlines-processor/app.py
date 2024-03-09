#!/usr/bin/env python3
"""
Script to read a large jsonl file and convert it to Netcool ASM File Observer Format
"""
__author__ = "Avinash Krishnan"

import os
from functools import wraps
import time
import logging
import json
import pandas as pd


# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)


def timeit(func):
    """
    Decorator function to measure the execution time of another function.
    Reference: freecodecamp
    """
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.debug(f'Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper


def set_province(location):
    """
    A dummy function to map location to province.
    """
    province_map = {
        'Melbourne': 'Victoria',
        'Adelaide': 'South Australia',
        'Sydney': 'New South Wales',
        'Canberra': 'Federal Capital'
    }
    return province_map.get(location, 'Not Available')


def set_additional_fields(row):
    """
    Function to set additional fields for ASM. This is specifically the place where we enrich the data.
    """
    a_type = b_type = c_type = "NotAvailable"
    entity_types = ['devices']
    dummy = 'this is a dummy entry'
    name = row['device'].upper()
    tags = ["python_observer", "sample_data"]
    version = row['device'].split('-')[1]

    if row['device'].startswith("ABC"):
        a_type = row['device'][0:3] + '-type'
        entity_types.append('type')

    elif row['device'].endswith("LMN") and "M" in row['deviceIdentificationId']:
        b_type = row['device'][0:3] + '-city'
        entity_types.append('city')
    else:
        c_type = row['device'][0:3]
        entity_types.append('private')

    return name, tags, version, a_type, b_type, c_type, entity_types, dummy


def process_data_frame(df):
    """
    Function to process the DataFrame to proper format required by ASM.
    """
    # Make a copy of only required fields
    # df = chunk[['device', 'deviceid', 'devicecode', "label", "City"]].copy()

    # Fields that can be directly mapped
    df = df.rename(columns={'deviceid': 'uniqueId', 'devicecode': 'deviceIdentificationId', 'City': 'location'})

    # How a single field can be created. For learning
    df['province'] = df['location'].apply(set_province)

    # df['entityTypes'] = df.apply(set_entity_type, axis=1)
    df[['name', 'tags', 'version', 'aType', 'bType', 'cType', 'entityTypes', 'dummy']
       ] = df.apply(set_additional_fields, axis=1, result_type='expand')
    # if order is important
    # df = df[['entityTypes','uniqueId','deviceIdentificationId','location','province','aType', 'bType', 'cType']]
 # Replace 'None' values in specified columns with 'pd.NA'
    # Specify columns to check for None values
    columns_to_check = ['name', 'aType', 'bType', 'cType']

    df[columns_to_check] = df[columns_to_check].map(
        lambda x: None if pd.isna(x) or x == '' or x == 'NotAvailable' else x)

    # Drop columns with all missing values
    df = df.dropna(axis=1, how='all')
    return df

# Function to convert DataFrame row to JSON object


def row_to_json(row):
    """
    Function to convert to json.
    Removes null columns
    Pandas use ujson which do undesired string escaping. Hence using json.dumps
    """
    row_without_null_columns = row.dropna()
    # Convert the modified row to a dictionary
    row_dict = row_without_null_columns.to_dict()
    # Convert the dictionary to a JSON string
    json_string = json.dumps(row_dict)
    return f'v: {json_string}\n'


@timeit
def generate_asm_using_loads(input_file_path, output_file_path):

    count = 0
    with open(output_file_path, 'w', encoding='utf-8') as file2:

        with pd.read_json(input_file_path, lines=True, chunksize=25000) as reader:
            for chunk in reader:
                count += len(chunk.index)
                logger.info(f'Processed {count} rows so far..')
                df = process_data_frame(chunk)
                json_lines = df.apply(row_to_json, axis=1)
                file2.writelines(json_lines)

    logger.info(f"Total Rows in input csv {count}")


if __name__ == "__main__":
    cwd = os.getcwd()
    input_file = os.path.join(cwd, "input/device-details.jsonl")
    output_file = os.path.join(cwd, "output/device-details.jsonl")
    generate_asm_using_loads(input_file, output_file)
