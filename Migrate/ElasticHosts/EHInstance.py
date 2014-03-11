"""
EHInstance
~~~~~~~~~~~~~~~~~

This module provides EHInstance class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback


import logging
import traceback
import VmInstance
import socket
import requests

class EHInstance(VmInstance.VmInstance):
    """ElasticHosts server instance"""

    def __init__(self , intanceid, eh_connection):
        """
        intiializes EH instance via it's id and requests module connection
        """
        self.__instanceId = instance_id
        self.__EH = eh_connection

    def run(self):
        """starts instance"""
        response = self.__EH.post(self.__hostname+"/servers/"+self.__instanceId+"/start")
        if response.status_code != 204:
            logging.warning("!Unexpected status code returned by the ElasticHosts request: " + str(response) + " " + str(response.text))
            logging.warning("Headers: %s \n" , str(response.request.headers) )
            response.raise_for_status()
        return

    def stop(self):
        """stops instance"""
        response = self.__EH.post(self.__hostname+"/servers/"+self.__instanceId+"/stop")
        if response.status_code != 204:
            logging.warning("!Unexpected status code returned by the ElasticHosts request: " + str(response) + " " + str(response.text))
            logging.warning("Headers: %s \n" , str(response.request.headers) )
            response.raise_for_status()
        return
        return

    def __str__(self):
        """gets string representation of instance"""
        return "ElasticHosts server "+str(self.__instanceId)


    def attachDataVolume(self):
        """attach data volume"""
        #TODO implement
        return

    def getIp(self):
        response = self.__EH.get(self.__hostname+"/servers/"+self.__instanceId+"/info")
        if response.status_code != 200:
               logging.warning("!Unexpected status code returned by the ElasticHosts request: " + str(response) + " " + str(response.text))
               logging.warning("Headers: %s \n" , str(response.request.headers) )
               response.raise_for_status()
        ip = response.json()[u'nic:0:dhcp:ip"']
        return ip