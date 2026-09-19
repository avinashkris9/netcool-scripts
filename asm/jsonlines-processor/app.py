#!/usr/bin/env python3
"""
Script to read a large jsonl file and convert it to Netcool ASM File Observer Format
"""
__author__ = "Avinash Krishnan"
from pandarallel import pandarallel

import os
import sys
from functools import wraps
import time
import logging
import json
import pandas as pd
from pathlib import Path
import multiprocessing as mp
pandarallel.initialize(progress_bar=True, nb_workers=6)
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


# Function to convert DataFrame row to JSON object
def process_edge_ids(edge_file_path, id_list):

    chunk = pd.read_json(edge_file_path,  encoding='utf-8', lines=True)
    df = chunk[chunk['from_device'].isin(id_list)]

    return df


def merge_edges(vertex_df, edge_df):

    def applier(row):
        ids = []
        relationships = []

        if isinstance(row['to_device'], list):
            ids.extend(row['to_device'])
        if 'to_device_new' in row and isinstance(row['to_device_new'], list):
            ids.extend(row['to_device_new'])
        if isinstance(row.get('edge_type'), list):
            relationships.extend(row['edge_type'])
        if isinstance(row.get('edge_type_new'), list):
            relationships.extend(row['edge_type_new'])

        return ids, relationships

    c = edge_df.groupby('from_device', as_index=False).agg(list)

    vertex_df = vertex_df.merge(c, left_on="device", right_on="from_device", how='left',
                                suffixes=('', '_new'))

    vertex_df[['to_device', "edge_type"]] = vertex_df.apply(applier, axis=1, result_type='expand')

    return vertex_df


def process_edges(edge_file_path, vertex_df):
    count = 0

    def applier(row):
        ids = []
        relationships = []

        if isinstance(row['to_device'], list):
            ids.extend(row['to_device'])
        if 'to_device_new' in row and isinstance(row['to_device_new'], list):
            ids.extend(row['to_device_new'])
        if isinstance(row.get('edge_type'), list):
            relationships.extend(row['edge_type'])
        if isinstance(row.get('edge_type_new'), list):
            relationships.extend(row['edge_type_new'])

        return ids, relationships

    with pd.read_json(edge_file_path,  encoding='utf-8', lines=True, chunksize=25000) as x:

        for chunk in x:

            count += len(chunk.index)
            logger.info(f'Edge Processed {count} rows so far..')
            c = chunk.groupby('from_device', as_index=False).agg(list)
            # chunk = chunk.groupby('from_device').apply(
            #     lambda x: x[['to_device', 'edge_type']].values.tolist()).reset_index(name='Values')
            # print(c)
            vertex_df = vertex_df.merge(c, left_on="device", right_on="from_device", how='left',
                                        suffixes=('', '_new'))
            # merge causes duplicates. so hack it
            # print(vertex_df.to_string())

            vertex_df[['to_device', "edge_type"]] = vertex_df.apply(applier, axis=1, result_type='expand')

    return vertex_df


@timeit
def process_data_frame(df, edge_file):
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

    # Drop columns with all missing values
    df = df.dropna(axis=1, how='all')

    columns_to_check = ['name', 'aType', 'bType', 'cType']
    df[columns_to_check] = df[columns_to_check].map(
        lambda x: None if pd.isna(x) or x == '' or x == 'Not Available' or not x or len(x) == 0 else x)
    # df = process_edges(edge_file, df)
    pool = mp.Pool()  # use 4 processes
    funclist = []
    uniqueIds = df['device'].tolist()
    for filename in os.listdir("input/edges"):
        f = os.path.join("input/edges", filename)
        # process each data frame
        r = pool.apply_async(process_edge_ids, [f, uniqueIds])
        funclist.append(r)
    pool.close()
    pool.join()
    x = []
    for f in funclist:
        x.append(f.get())  # timeout in 10 seconds

    edges = pd.concat(x)
    df = merge_edges(df, edges)

    return df


def row_to_json(row):
    """
    Function to convert to json.
    Removes null columns
    Pandas use ujson which do undesired string escaping. Hence using json.dumps
    """

    # Convert the modified row to a dictionary
    row_without_null_columns = row.dropna()
    edge_references = []

    if 'to_device' in row_without_null_columns and row_without_null_columns['to_device']:
        for device, relationship in zip(row_without_null_columns['to_device'], row_without_null_columns['edge_type']):
            edge_references.append({"_toUniqueId": device, "edgeType": relationship})
        row_without_null_columns['references'] = edge_references
        row_without_null_columns.drop(['to_device', 'edge_type'])
    # Convert the dictionary to a JSON string
  # Drop 'toIds_x' and 'toIds_y' columns only if they exist
    row_without_null_columns.drop(labels=[col for col in row_without_null_columns.index.to_list() if col in [
        "to_device", "edge_type", "from_device"]], inplace=True, errors='ignore')

    row_dict = row_without_null_columns.to_dict()
    json_string = json.dumps(row_dict)
    return f'v: {json_string}\n'


@timeit
def generate_asm_using_loads(input_file_path, output_file_path, edge_file):

    count = 0
    with open(output_file_path, 'w+', encoding='utf-8') as file2:

        with pd.read_json(input_file_path, lines=True, chunksize=25000) as reader:
            for chunk in reader:
                count += len(chunk.index)
                logger.info(f'Processed {count} rows so far..')
                df = process_data_frame(chunk, edge_file)
              #  json_lines = df.parallel_apply(row_to_json, axis=1)
                pool = mp.Pool()
                results = []
                for json_str in pool.imap_unordered(row_to_json, [row for _, row in df.iterrows()]):
                    results.append(json_str)
                pool.close()
                pool.join()

                file2.writelines(results)

    logger.info(f"Total Rows in input csv {count}")


if __name__ == "__main__":
    cwd = os.getcwd()
    input_file = os.path.join(cwd, "input/device-details.jsonl")
    output_file = os.path.join(cwd, "output/device-details.jsonl")
    edge_file = os.path.join(cwd, "input/edges.jsonl")

    generate_asm_using_loads(input_file, output_file, edge_file)
