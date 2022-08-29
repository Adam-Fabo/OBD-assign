#!/usr/bin/env python3

""" Script loads data from car using OBD
    Author: Adam Fabo
    Date: 16.8.2022
"""

import obd

def func():
    # connecting to emulator
    # https://github.com/Ircama/ELM327-emulator
    connection = obd.OBD("/dev/pts/0")

    # get Diagnostic Trouble Codes
    cmd = obd.commands.GET_DTC  # GET_CURRENT_DTC
    response = connection.query(cmd)
    print("Trouble codes of a car: " + str(response.value))


    # control module voltage is supposed to be the same as the battery voltage
    # https://stackoverflow.com/questions/54568402/how-to-get-the-battery-voltage-of-a-car-using-obd-2
    cmd = obd.commands.CONTROL_MODULE_VOLTAGE
    response = connection.query(cmd)
    print("Battery voltage is: " + str(response.value))



if __name__ == "__main__":
    func()