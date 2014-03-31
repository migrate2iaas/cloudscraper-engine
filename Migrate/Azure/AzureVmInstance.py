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
        info =  self.__vmConnection.get_vm_info(self.__instanceId)
        for conf_set in info.configuration_sets:
            logging.debug("Got Azure configuration set from VM " + str(self.__instanceId))
            logging.debug("Configuration set: " + str(vars(conf_set)))
            if str(conf_set.configuration_set_type) == 'NetworkConfiguration':
                for endpoint in conf_set.input_endpoints:
                    if endpoint.Vip:
                        return endpoint.Vip
        return None
