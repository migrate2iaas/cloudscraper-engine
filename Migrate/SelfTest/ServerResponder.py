"""
ServerResponder
~~~~~~~~~~~~~~~~~

This module provides ServerResponder class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback



class ServerResponder(object):
    """base interface to test response of the migrated server"""

    def __init__ (self , timeout_seconds):
        """
        Constructor
            
        Args:
            timeout_seconds - timeout in seconds for each attempt to wait for server response

        """
        self.__timeoutSeconds = timeout_seconds
    def waitResponseByMachineName(self):
        """waits till response is done: by using machine name"""
        return
    def waitResponseByIp(self , ip):
        """waits till response is done by ip"""
        return 

    def getTimeout(self):
        """returns timeout in seconds"""
        return self.__timeoutSeconds
