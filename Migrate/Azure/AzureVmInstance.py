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
import time

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

        timeleft = 600
        recheck_interval = 20
        #try to check several times until ip is allocated
        while timeleft > 0:
            timeleft = timeleft - recheck_interval
            svc = self.__vmConnection.get_vm_service(self.__instanceId)
            for deployment in svc.deployments:
                logging.debug("Got Azure cloud service deployment from VM " + str(self.__instanceId))
                logging.debug("Deployment: " + str(vars(deployment)))
                for endpoint in deployment.input_endpoint_list:
                    logging.debug("Got endpoint: " + str(endpoint))
                    if endpoint.vip:
                        return str(endpoint.vip)

                for role in deployment.role_instance_list:
                    logging.debug("Role: " + str(vars(role)))
                    for instance_endpoint in role.instance_endpoints:
                        logging.debug("Endpoint: " + str(vars(instance_endpoint)))
                        if instance_endpoint.vip:
                            # try to get public ip
                            return str(instance_endpoint.vip)
                    # if ip couldn't be got try getting any address we could get
                    return str(role.ip_address)
            time.sleep(recheck_interval)

        logging.debug("!!!ERROR failed to find machine ip to test it's work")
        return None
