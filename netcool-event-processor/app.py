#!/usr/bin/env python3

import requests
import logging
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


OBJECTSERVER_HOST_NAME = os.getenv("OBJECTSERVER_HOST_NAME", "DESKTOP-KHGPQP6.local")
OBJECTSERVER_REST_API_PORT = os.getenv("OBJECTSERVER_REST_API_PORT", 9090)
OBJECTSERVER_API_USER = os.getenv("OBJECTSERVER_API_USER", "root")
OBJECTSERVER_API_PASSWORD = os.getenv("OBJECTSERVER_API_PASSWORD", "")
BASE_URL = f'http://{OBJECTSERVER_HOST_NAME}:{OBJECTSERVER_REST_API_PORT}/objectserver'
auth = (OBJECTSERVER_API_USER, OBJECTSERVER_API_PASSWORD)
BASE_URI = f'{BASE_URL}/restapi'


def pp(data):

    json_str = json.dumps(data, indent=4)
    return json_str


def validate_connection():
    logger.debug(f'Requesting objectserver systeminfo')

    try:
        response = requests.get(f'{BASE_URL}/sysinfo', auth=auth)
        response.raise_for_status()
        data = response.json()
        logging.debug("Data Retrieved Successfully: %s", data)
    except requests.exceptions.RequestException as e:
        logging.error("Failed to retrieve data: %s", e)
        logging.error(response.text)


def get_event_data(where_clause: str = "", coloumn_list: list = []) -> list:
    logger.info(f'Requesting objectserver alarms with filter {where_clause} and columns {str(coloumn_list)}')
    response = get_events(where_clause, ",".join(coloumn_list))
    data = response.get('rowset', {}).get('rows', [])
    logging.info("Obtained %s rows", response.get('rowset', {}).get('affectedRows'))
    return data


def get_events(where_clause: str = "", coloumn_list: str = ""):

    params = {}
    response_json = {}
    if where_clause:
        params["filter"] = where_clause
    if coloumn_list:
        params["collist"] = coloumn_list
    try:
        response = requests.get(f'{BASE_URI}/alerts/status', auth=auth, params=params)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        logging.error("Failed to retrieve data: %s", e)
        logging.error(response.text)

    return response_json


def read_file(input_file):
    with open(input_file, encoding="utf-8") as sf:
        parsed_json = json.load(sf)
        return parsed_json


def generate_event(coldesc_list, rows):
    return {
        "rowset": {
            "coldesc": coldesc_list,
            "rows": [rows]
        }
    }


def create_event(event_data):
    # logger.info(f'Create event %s', pp(generate_event()))
    headers = {'content-type': 'application/json', 'Accept': 'application/json'}

    try:
        response = requests.post(f'{BASE_URI}/alerts/status', auth=auth, headers=headers, data=event_data)
        response.raise_for_status()
        data = response.json()
        logging.info("Obtained %s rows", response.json())
    except requests.exceptions.RequestException as e:
        logging.error("Failed to retrieve data: %s", e)
        logging.error(response.text)
    return data


if __name__ == '__main__':

    SCHEMA_FILE_NAME = "schema.json"
    ALERTS_FILE_NAME = "alerts.json"
    schema = read_file(SCHEMA_FILE_NAME)
    alerts = read_file(ALERTS_FILE_NAME)['alerts']
    status_col_description = schema['alerts.status']
    for alert in alerts:
        print(alert)
        event = pp(generate_event(status_col_description, alert))
        create_event(event_data=event)
    p = get_event_data(where_clause="AlertGroup='LinkStatus'")
    print(pp(p))
