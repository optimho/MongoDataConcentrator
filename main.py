# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import paho.mqtt.client as mqtt
import json
import pymongo
import binascii
import struct
import datetime
import logging
from systemdlogging.toolbox import init_systemd_logging

# This one line in most cases would be enough.
# By default it attaches systemd logging handler to a root Python logger.
init_systemd_logging()  # Returns True if initialization went fine.

# Now you can use logging as usual.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


IP = '172.105.169.226'
RDNS = 'li2078-226.members.linode.com'


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("iotnz-api/mqtt/things/70B3D54991894DF6/uplink")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    #print(msg.topic+" "+str(msg.payload))
    #data = str(msg.payload).split(',')
    jsonData = json.loads(msg.payload)
    logger.info('Jason Data ', jsonData)
    # print("Customer ID {}".format(jsonData["DevEUI_uplink"]["CustomerID"]))
    # print("Device ID {}".format(jsonData["DevEUI_uplink"]["DevEUI"]))
    # print("Pay Load {}".format(jsonData["DevEUI_uplink"]["payload_hex"]))

    pay_load = jsonData["DevEUI_uplink"]["payload_hex"]
    pay_load = binascii.unhexlify(pay_load)
    temperature, dewpoint = struct.unpack('ff', pay_load)
    deviceID = jsonData["DevEUI_uplink"]["CustomerID"]
    logger.info("Temperature {}".format(temperature))
    logger.info("dew point {}".format(dewpoint))
    add_device_data(deviceID, temperature, dewpoint)



def add_device_data(deviceID, temperature, dewpoint):
    """
    Device information and data
    :param deviceID:
    :param temperature:
    :param dewpoint:
    :return:
    """

    document = {
        'DeviceID': deviceID,
        'Temperature': temperature,
        'Dewpoint': dewpoint,
        'Time': datetime.datetime.now()
    }
    return device.insert_one(document)


mongo_client = pymongo.MongoClient("mongodb://Optimho:Blackmamba#1968@li2078-226.members.linode.com/admin")
print('Hello')
print(mongo_client.list_database_names())
db = mongo_client['IoT']

device = db['device']

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("li2078-226.members.linode.com", 1883)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.

client.loop_forever()
