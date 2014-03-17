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
        ip = self.getIp()
        port = 3389

        while timeout:
            try:
                sock = socket.create_connection((ip,port) , timeout)
                sock.close()
                return True
            except Exception as e:
                logging.error("!!!ERROR: Failed to probe the remote server for RDP connection!")
                logging.error("!!!ERROR:" + str(e))
                logging.error(traceback.format_exc())
                
                time.sleep(timeout)
                timeout = timeout - timeout

        return True

    def attachDataVolume(self):
        """attach data volume"""
        raise NotImplementedError

    def getIp(self):
        """returns public ip string"""
        raise NotImplementedError


