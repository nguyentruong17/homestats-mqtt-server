#!/usr/bin/env python3

# from botocore.config import Config
from datetime import datetime
# import boto3
# import json
import os
import sqlite3

# PATH
BASE_DIR = os.path.join(os.path.dirname( __file__ ), '..')

# DATETIME
DT_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

# MQTT
MQTT_HOST = '192.168.0.62'
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
TOPIC = 'bedroom/#'

# SQLITE
DATABASE_FILE = 'multisensors.db'
SQLITE_TABLE_NAME = 'multi_sensors_data'

# AWS
AWS_PROFILE = 'test'
DATABASE_NAME = 'testsensors'
MAX_RECORDS_PER_WRITE = 100
REGION_NAME = 'us-east-2'
TABLE_NAME = 'multimetrics'
            
# SENSORS
SENSOR_NAMES_SET = {
    'sys_cpu__utilization',
    'sys_mem__usage',
    'sys_gpu__mem_clock',
    'sys_gpu__utilization',
    'sys_gpu__mem_usage',
    'temp_mobo__measure',
    'temp_chipset__measure',
    'temp_gpu__measure',
    'temp_gpu__hotspot',
    'fan_cpu__measure',
    'fan_gpu__fan1',
    'fan_gpu__fan2',
    'fan_gpu__fan3',
    'voltage_gpu__measure',
    'wattage_gpu__measure',
    'sys_cpu__clock_core_max',
    'sys_cpu__clock_core_min',
    'sys_cpu__utilization_thread_max',
    'sys_cpu__utilization_thread_min',
    'temp_hdd__hdd1',
    'temp_hdd__hdd2'
}
SORTED_SENSORS_LIST = sorted(list(SENSOR_NAMES_SET))

# def print_rejected_records_exceptions(err):
#     print("RejectedRecords: ", err)
#     for rr in err.response["RejectedRecords"]:
#         print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
#     if "ExistingVersion" in rr:
#         print("Rejected record existing version: ", rr["ExistingVersion"])

# def get_aws_write_client():
#     profile_name=os.environ['AWS_PROFILE']
    
#     session = boto3.Session(profile_name=profile_name)

#     write_client = session.client(
#             'timestream-write',
#             region_name=REGION_NAME,
#             config=Config(read_timeout=20,
#                 max_pool_connections=5000,
#                 retries={'max_attempts': 10}
#             )
#         )
    
#     return write_client

# def prepare_common_attributes():
#     common_attributes = {
#         'Dimensions': [
#             {'Name': 'hostname', 'Value': 'Torrent7'}
#         ],
#         'MeasureName': 'metric',
#         'MeasureValueType': 'MULTI'
#     }
    
#     return common_attributes

# def prepare_measure(measure_name, measure_value):
#     measure = {
#         'Name': measure_name,
#         'Value': str(measure_value),
#         'Type': 'DOUBLE'
#     }
    
#     return measure

# def write_records(rows):
#     write_client = get_aws_write_client()

#     if (write_client is None): return
    
#     records = []

#     for row in rows:
#         sqlite_id, timestamp, *rest = row
        
#         dt_obj = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
#         time = int(round(dt_obj.timestamp() * 1000))
        
#         record = {
#             'Time': str(time),
#             'MeasureValues': []
#         }
        
#         for index in range(len(SORTED_SENSORS_LIST)):
#             metric = SORTED_SENSORS_LIST[index]
#             value = rest[index]
#             measure = prepare_measure(metric, value)
            
#             record['MeasureValues'].append(measure)
              
#         records.append(record)
     
#     print(f'numrecords: {len(records)}')
#     if (len(records) == 0):
#         print('No record to write.')
#         return
    
#     print(f'lastrecod: {records[-1]}')
    
#     # https://www.geeksforgeeks.org/break-list-chunks-size-n-python/
#     batches = [records[i * MAX_RECORDS_PER_WRITE:(i + 1) * MAX_RECORDS_PER_WRITE] for i in range((len(records) + MAX_RECORDS_PER_WRITE - 1) // MAX_RECORDS_PER_WRITE )]
#     print(f'numbatches: {len(batches)}')
#     common_attributes = prepare_common_attributes()

#     print('WriteRecords in progress...')    
#     for batch in batches: 
#         try:
#             result = write_client.write_records(
#                 DatabaseName=DATABASE_NAME,
#                 TableName=TABLE_NAME,
#                 Records=batch,
#                 CommonAttributes=common_attributes
#             )
#             print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
#         except write_client.exceptions.RejectedRecordsException as err:
#             print_rejected_records_exceptions(err)
#         except Exception as err:
#             print("Error:", err)

# def publish_remote(userdata):
#     db_conn = userdata['db_conn']
#     sql = f"SELECT * FROM {SQLITE_TABLE_NAME} WHERE timestamp >= datetime('now', '-24 hour')"
#     cursor = db_conn.cursor()
#     cursor.execute(sql)
    
#     rows = cursor.fetchall()
#     db_conn.commit()
#     cursor.close()
    
#     print(f'numrows: {len(rows)}')
#     print('WriteRecords started')
#     write_records(rows)
#     print('WriteRecords completed')

# def clean_local(userdata):
#     db_conn = userdata['db_conn']
#     sql = f"SELECT * FROM {SQLITE_TABLE_NAME} WHERE timestamp BETWEEN datetime('now', '-4 day') AND datetime('now', '-3 day')"
#     cursor = db_conn.cursor()
#     cursor.execute(sql)
    
#     db_conn.commit()
#     cursor.close()

def main():
    # Initialize SQLITE3
    # db_conn = sqlite3.connect(DATABASE_FILE)
    # sql = f"SELECT * FROM {SQLITE_TABLE_NAME} WHERE timestamp BETWEEN datetime('now', '-2 day') AND datetime('now', '-1 day')"
    # cursor = db_conn.cursor()
    # cursor.execute(sql)
    # rows = cursor.fetchall()
    # print(f'numrows: {len(rows)}')
    # db_conn.commit()
    # cursor.close()
    
    print('clearing local')

main()
