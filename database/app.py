#!/usr/bin/env python3
import boto3
from botocore.config import Config
import configparser
from datetime import datetime
import json
import os
import paho.mqtt.client as mqtt
import re
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

def print_rejected_records_exceptions(err):
    print("RejectedRecords: ", err)
    for rr in err.response["RejectedRecords"]:
        print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
    if "ExistingVersion" in rr:
        print("Rejected record existing version: ", rr["ExistingVersion"])

def get_aws_write_client():
    profile_name=os.environ['AWS_PROFILE']
    
    session = boto3.Session(profile_name=profile_name)
        
    write_client = session.client(
            'timestream-write',
            region_name=REGION_NAME,
            config=Config(read_timeout=20,
                max_pool_connections=5000,
                retries={'max_attempts': 10}
            )
        )
    
    return write_client

def write_records(rows):
    write_client = get_aws_write_client()

    if (write_client is None): return
    
    records = []

    for row in rows:
        # print(row)
        sqlite_id, timestamp, rgroup, sensor, measure_name, measure_value = row
        
        dt_obj = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
        time = int(round(dt_obj.timestamp() * 1000))
        
        dimensions = [
            {'Name': 'rgroup', 'Value': rgroup},
            {'Name': 'sensor', 'Value': sensor} 
        ]
        
        record = {
            'Dimensions': dimensions,
            'MeasureName': measure_name,
            'MeasureValue': str(measure_value),
            'MeasureValueType': 'MULTI',
            'Time': str(time)
        }
        
        print(record)
        
        records.append(record)
        
    if (len(records) == 0):
        print('No record to write.')
        return
    
    print('Writing records')
    
    # https://www.geeksforgeeks.org/break-list-chunks-size-n-python/
    batches = [records[i * MAX_RECORDS_PER_WRITE:(i + 1) * MAX_RECORDS_PER_WRITE] for i in range((len(records) + MAX_RECORDS_PER_WRITE - 1) // MAX_RECORDS_PER_WRITE )]
    
    for batch in batches: 
        try:
            result = write_client.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_NAME,
                Records=batch,
                CommonAttributes={}
            )
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except write_client.exceptions.RejectedRecordsException as err:
            print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)
    
def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))

    client.subscribe(TOPIC)
    
    # db_conn = userdata['db_conn']
    # sql = "SELECT * FROM sensors_data WHERE timestamp >= datetime('now', '-1 hour')"
    # cursor = db_conn.cursor()
    # cursor.execute(sql)
    
    # rows = cursor.fetchall()
    
    # write_records(rows)

def on_disconnect(client):
    print('Disconnected')

def on_message(client, userdata, msg):
    message=msg.payload.decode('utf-8')
    # print(message)
    
    payloadJson = json.loads(message)
    timestamp = payloadJson['sent']
    sensors = payloadJson['payload']
    
    db_conn = userdata['db_conn']
    
    sensorsDict = dict.fromkeys(SENSOR_NAMES_SET, -1)
    sensorsDict['timestamp'] = timestamp
    
    for each in sensors:
        key = each['Id']
        value = each['Value']
        
        if key in sensorsDict:
            sensorsDict[key] = value
    
    sensors = list(SENSOR_NAMES_SET)
    cols = ', '.join(sensors)
    
    vals = map(lambda col: ':' + col, sensors)
    vals = ', '.join(vals)
    
    sql = f"""INSERT INTO {SQLITE_TABLE_NAME}
        (
            timestamp,
            {cols}
        ) VALUES
        (
            :timestamp,
            {vals}
        )
    """

    cursor = db_conn.cursor()
    cursor.execute(sql, sensorsDict)
    db_conn.commit()
    cursor.close()

def main():
    cols = list(SENSOR_NAMES_SET)
    cols = map(lambda col: col + ' REAL NOT NULL', cols)
    sql = ', '.join(cols)
    
    # Initialize SQLITE3
    db_conn = sqlite3.connect(DATABASE_FILE)
    sql = f"""
        CREATE TABLE IF NOT EXISTS {SQLITE_TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            {sql}
        )
    """
    
    cursor = db_conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS {SQLITE_TABLE_NAME}')
    cursor.execute(sql)
    cursor.close()

    # Initialize the client that should connect to the Mosquitto broker
    print('Before Init MQTT')
    client = mqtt.Client()
    
    client.user_data_set({'db_conn': db_conn})

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)

    # Blocking loop to the Mosquitto broker
    client.loop_forever()

main()
