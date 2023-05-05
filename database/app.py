#!/usr/bin/env python3
import boto3
from botocore.config import Config
import configparser
import json
import os
import paho.mqtt.client as mqtt
import re
import sqlite3
import sys

# MQTT
MQTT_HOST = '192.168.0.62'
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
TOPIC = 'bedroom/#'

# SQLITE
DATABASE_FILE = 'sensors.db'

# AWS
AWS_PROFILE = 'test'

def get_aws_write_client():
    # parse the aws credentials file
    path = os.environ['HOME'] + '/.aws/credentials'
    config = configparser.ConfigParser()
    config.read(path)

    # read in the aws_access_key_id and the aws_secret_access_key
    # if the profile does not exist, error and exit
    if AWS_PROFILE in config.sections():
        aws_access_key_id = config[AWS_PROFILE]['aws_access_key_id']
        aws_secret_access_key = config[AWS_PROFILE]['aws_secret_access_key']
        aws_region = config[AWS_PROFILE]['aws_region'] or 'us-east-1'
    else:
        print("Cannot find profile '{}' in {}".format(AWS_PROFILE, path), True)
        return None

    # if we don't have both the access and secret key, error and exit
    if aws_access_key_id is None or aws_secret_access_key is None:
        print("AWS config values not set in '{}' in {}".format(AWS_PROFILE, path), True)
        return None
    
    session = boto3.Session()
        
    write_client = session.client(
            'timestream-write',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
            config=Config(read_timeout=20,
                max_pool_connections=5000,
                retries={'max_attempts': 10}
            )
        )
    
    return write_client

def write_records(rows):
    write_client = get_aws_write_client()
    if (write_client is None):
        return
    
    for row in rows:
        print(row)
    
def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))

    client.subscribe(TOPIC)
    
    db_conn = userdata['db_conn']
    sql = "SELECT * FROM sensors_data WHERE timestamp <= datetime('now', '-1 day')"
    cursor = db_conn.cursor()
    cursor.execute(sql)
    
    rows = cursor.fetchall()
    

def on_disconnect(client):
    print('Disconnected')

def on_message(client, userdata, msg):
    message=msg.payload.decode('utf-8')
    print(message)
    
    payloadJson = json.loads(message)
    timestamp = payloadJson['sent']
    sensors = payloadJson['payload']
    
    db_conn = userdata['db_conn']
    
    for each in sensors:
        id = each['Id']
        measure_value = each['Value']
        
        parts = re.split('\[(.*?)\]', id)
        
        segments = filter(lambda part: bool(part), parts)
        
        group, sensor, measure_name = segments
        
        measure_name = measure_name[1:]
        
        sql = 'INSERT INTO sensors_data (timestamp, rgroup, sensor, measure_name, measure_value) VALUES (?, ?, ?, ?, ?)'
        cursor = db_conn.cursor()
        cursor.execute(sql, (timestamp, group, sensor, measure_name, measure_value))
    
    db_conn.commit()
    cursor.close()

def main():
    # Initialize SQLITE3
    db_conn = sqlite3.connect(DATABASE_FILE)
    sql = """
        CREATE TABLE IF NOT EXISTS sensors_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            rgroup TEXT NOT NULL,
            sensor TEXT NOT NULL,
            measure_name TEXT NOT NULL,
            measure_value REAL NOT NULL
        )
    """
    cursor = db_conn.cursor()
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
