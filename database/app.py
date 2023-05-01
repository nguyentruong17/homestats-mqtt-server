#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import sqlite3
import re
import json

# MQTT
MQTT_HOST = '192.168.0.62'
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
TOPIC = 'bedroom/#'

# SQLITE
DATABASE_FILE = 'sensors.db'

def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))
    client.subscribe(TOPIC)

def on_disconnect(client):
    print('Disconnected')

def on_message(client, userdata, msg):
    message=msg.payload.decode('utf-8')
    print(message)
    
    payloadJson = json.load(message)
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
        
        sql = 'INSERT INTO sensors_data (timestamp, group, sensor, measure_name, measure_value) VALUES (?, ?, ?, ?, ?)'
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
            group TEXT NOT NULL,
            sensor TEXT NOT NULL,
            measure_name TEXT NOT NULL
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
