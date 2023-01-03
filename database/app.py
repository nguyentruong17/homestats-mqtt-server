#!/usr/bin/env python3
import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("bedroom/#")

def on_message(client, userdata, msg):
    message=msg.payload.decode("utf-8")
    print(client, userdata, message)


# Initialize the client that should connect to the Mosquitto broker
print('Before Init MQTT')
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("192.168.0.62", 1883, 60)

# Blocking loop to the Mosquitto broker
client.loop_forever()
