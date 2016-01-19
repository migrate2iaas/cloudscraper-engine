"""
VmInstance
~~~~~~~~~~~~~~~~~

This module provides VmInstance class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import socket
import time


class VmInstance(object):
    """abstract class for Virtual machine instance in the cloud"""

    def __init__(self , vm_id = None):
        self.__vmId = vm_id

    def run(self):
        """starts instance"""
        raise NotImplementedError

    def stop(self):
        """stops instance"""
        raise NotImplementedError


    def getId(self):
        """returns cloud id of the instance"""
        return self.__vmId

    def finalize(self):
        """finalizes the VM setting it to stopped state ready to be boot whenever user starts it"""
        self.stop()

    def checkAlive(self, timeout = 1500 , port = 3389):
        """
        Performs RDP\ssh probe for an instance
        Args:
            timeout: int - is timeout to wait in seconds
            port: int - port to check
        """
        time_retry = 90
        # ugly c-style loop 
        while 1:
            try:
                ip = self.getIp()
                if not ip:
                    logging.warning("!Failed to obtain ip address")
                else:
                    logging.info("Probing " + str(ip) + ":" + str(port) + " for connectivity")
                    sock = socket.create_connection((ip,port) , timeout)
                    sock.close()
                    logging.info("Server " + str(ip) + ":" + str(port) + " successfully responded")
                    return True
            except Exception as e:
                logging.error("!: Failed to probe the remote server for a connection!")
                logging.error("!:" + str(e))
                logging.error(traceback.format_exc())
            timeout = timeout - time_retry
            if timeout > 0:
                logging.info("--- Waiting more " + str(timeout) + " for it to respond");
                time.sleep(time_retry)
            else:
                break

        return False

    def getIp(self):
        """returns public ip string"""
        raise NotImplementedError

    def deallocate(self , subresources=True):
        """deallocates a VM
            Args:
            subresources: Boolean - if True, deallocates all associated resources (disks, ips). Deallocates only the vm itself otherwise
        """
        raise NotImplementedError

    def __str__(self):
        return str(self.getId())
