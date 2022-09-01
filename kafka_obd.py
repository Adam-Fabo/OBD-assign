#!/usr/bin/env python

""" Script loads data from car using OBD and sends them using kafka
    Script needs config file to establish kafka connection
    Change logs visibility using logging.setLevel
    Author: Adam Fabo
    Date: 1.9.2022
"""

from configparser import ConfigParser
from confluent_kafka import Producer
import obd

import logging
import json
import threading


# Optional per-message delivery callback (triggered by poll() or flush())
def delivery_callback(err, msg):
    if err:
        logging.error('ERROR: Message failed delivery: {}'.format(err))
    else:
        logging.info("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))



def voltage(producer, obd_connection, topic, mutex):
    # start next timer after 10 second
    timer = threading.Timer(10.0, voltage,[producer,obd_connection,topic,mutex])
    timer.start()

    cmd = obd.commands.CONTROL_MODULE_VOLTAGE

    # using semaphore when accessing car using OBD (It is not atomic operation)
    mutex.acquire()
    try:
        response = obd_connection.query(cmd)
    finally:
        mutex.release()


    if response.value is None:
        # if connection is lost cancel next thread and raise error
        if obd_connection.status() == 'Not Connected':
            timer.cancel()
            raise RuntimeError("OBD connection has been closed")
        else:
            logging.warning('Response was None but connection is OK')
            return

    producer.produce(topic, key = "voltage", value=str(float(response.value.magnitude)), callback=delivery_callback)

    # poll time depends on the application
    producer.poll(60)



def dtc(producer, obd_connection, topic, mutex):
    # start next timer after 60 second
    timer = threading.Timer(60.0, dtc,[producer,obd_connection,topic,mutex])
    timer.start()

    cmd = obd.commands.GET_DTC  # GET_CURRENT_DTC

    # using semaphore when accessing car using OBD (It is not atomic operation)
    mutex.acquire()
    try:
        response = obd_connection.query(cmd)
    finally:
        mutex.release()

    if response.value is None:
        # if connection is lost cancel next thread and raise error
        if obd_connection.status() == 'Not Connected':
            timer.cancel()
            raise RuntimeError("OBD connection has been closed")
        else:
            logging.warning('Response was None but connection is OK')
            return


    # take a look at the bytearray and the codes detected by the obd - obd ignores first 2 values
    # implementation of obd https://github.com/brendan-w/python-OBD/blob/master/obd/decoders.py#L413
    # it ignores first two bytes which should be mode and count bytes but in this case it is valid DTC
    # obd2 frame: https://www.csselectronics.com/pages/obd2-explained-simple-intro
    logging.debug(response.messages[0].data)
    logging.debug(str(response.value))

    # change codes to dict
    trouble_codes = {}
    for code in response.value:
        trouble_codes[code[0]] = code[1]


    # output trouble codes are in format {DTC_ID_1 : description_1, DTC_ID_2 : description_2, . . .}
    producer.produce(topic, key ="trouble_codes", value = json.dumps(trouble_codes), callback=delivery_callback)

    # poll time depends on the application
    producer.poll(60)





# use logging
# change topic
if __name__ == '__main__':

    # configuration for the kafka is in configuration file
    # https://developer.confluent.io/get-started/python/#configuration
    config_file = 'config.ini'
    topic = 'obd_info'
    obd_port = '/dev/pts/0'

    # for accessing car with multiple threads Lock (semaphore is needed)
    mutex = threading.Lock()

    logger_root = logging.getLogger()
    logger_root.setLevel(logging.WARNING)

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read(config_file)
    config = dict(config_parser['default'])

    producer = Producer(config)

    obd_connection = obd.OBD(obd_port)
    if obd_connection.status() == 'Not Connected':
        raise FileNotFoundError("OBD is not connected")

    # start first round of threads
    threading.Timer(1.0, dtc,[producer,obd_connection,topic,mutex]).start()
    threading.Timer(1.0, voltage,[producer,obd_connection,topic,mutex]).start()


