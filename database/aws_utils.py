#!/usr/bin/env python3

from botocore.config import Config
from constants import (
    CONFIG_MAX_ATTEMPTS,
    CONFIG_MAX_POOL_CONNECTION,
    CONFIG_READ_TIMEOUT,
    REGION_NAME
)
import boto3
import json
import os

def print_rejected_records_exceptions(err):
    print(f'RejectedRecords: {err}')
    for rr in err.response['RejectedRecords']:
        print(f"Rejected Index {rr['RecordIndex']}: {rr['Reason']}")
    if 'ExistingVersion' in rr:
        print(f"Rejected record existing version: {rr['ExistingVersion']}")

def get_aws_write_client():
    profile_name=os.environ['AWS_PROFILE']
    
    session = boto3.Session(profile_name=profile_name)

    write_client = session.client(
            'timestream-write',
            region_name=REGION_NAME,
            config=Config(
                read_timeout = CONFIG_READ_TIMEOUT,
                max_pool_connections = CONFIG_MAX_POOL_CONNECTION,
                retries={
                    'max_attempts': CONFIG_MAX_ATTEMPTS
                }
            )
        )
    
    return write_client

def prepare_common_attributes():
    common_attributes = {
        'Dimensions': [
            {'Name': 'hostname', 'Value': 'Torrent7'}
        ],
        'MeasureName': 'metric',
        'MeasureValueType': 'MULTI'
    }
    
    return common_attributes

def prepare_measure(measure_name, measure_value):
    measure = {
        'Name': measure_name,
        'Value': str(measure_value),
        'Type': 'DOUBLE'
    }
    
    return measure
