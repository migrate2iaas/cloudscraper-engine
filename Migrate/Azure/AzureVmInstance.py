"""
AzureVmInstance
~~~~~~~~~~~~~~~~~

This module provides AzureVmInstance class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import logging
import traceback
import VmInstance
import socket
import virtualmachine


class AzureVmInstance(VmInstance.VmInstance):
    """Class representing EC2 virutal server (instance)"""

    def __init__(self , instance_id , virtualmachine_connection):
        """azure vm instance constructor"""
        self.__instanceId = instance_id
        self.__vmConnection = virtualmachine_connection
        
        return 

    def run(self):
        """starts instance"""
        self.__vmConnection.start_vm(self.__instanceId)
        return

    def stop(self):
        """stops instance"""
        self.__vmConnection.stop_vm(self.__instanceId)
        return

    def __str__(self):
        """gets string representation of instance"""
        return "Azure VM "+str(self.__instanceId)


    def attachDataVolume(self):
        """attach data volume"""
        #TODO implement
        return

    def getIp(self):
        """gets public ip"""
        svc = self.__vmConnection.get_vm_service(self.__instanceId)
        for deployment in svc.deployments:
            logging.debug("Got Azure cloud service deployment from VM " + str(self.__instanceId))
            logging.debug("Deployment: " + str(vars(deployment)))
            if deployment.input_endpoint_list == None:
                continue
            for endpoint in deployment.input_endpoint_list:
                logging.debug("Endpoint: " + str(vars(endpoint)))
                if endpoint.vip:
                    logging.info("Got VM public ip:" + str(endpoint.vip));
                    return str(endpoint.vip)
        logging.debug("!!!ERROR failed to find machine ip to test it's work")
        return None
