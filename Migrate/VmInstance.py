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

    def run(self):
        """starts instance"""
        raise NotImplementedError

    def stop(self):
        """stops instance"""
        raise NotImplementedError


    def checkAlive(self, timeout = 500):
        """
        Performs RDP check for an instance
        Args:
            timeout: int - is timeout to wait in seconds
        """
        time_retry = 90
        # ugly c-style loop 
        while 1:
            try:
                ip = self.getIp()
                port = 3389
                if not ip:
                    logging.warning("!Failed to obtain ip address")
                else:
                    logging.info("Probing " + str(ip) + ":" + str(port) + " for connectivity")
                    sock = socket.create_connection((ip,port) , timeout)
                    sock.close()
                    logging.info("Server " + str(ip) + ":" + str(port) + " successfully responded")
                    return True
            except Exception as e:
                logging.error("!: Failed to probe the remote server for RDP connection!")
                logging.error("!:" + str(e))
                logging.error(traceback.format_exc())
            timeout = timeout - time_retry
            if timeout > 0:
                logging.info("--- Waiting more " + str(timeout) + " for it to respond");
                time.sleep(time_retry)
            else:
                break

        return False

    def attachDataVolume(self):
        """attach data volume"""
        raise NotImplementedError

    def getIp(self):
        """returns public ip string"""
        raise NotImplementedError

    def deallocate(self , subresources=True):
        """deallocates a VM
            Args:
            subresources: Boolean - if True, deallocates all associated resources (disks, ips). Deallocates only the vm itself otherwise
        """
        raise NotImplementedError

