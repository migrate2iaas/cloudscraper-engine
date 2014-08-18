"""
LinuxAdjustOptions
~~~~~~~~~~~~~~~~~

This module provides LinuxAdjustOptions class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback

import random
import SystemAdjustOptions
import os

class LinuxAdjustOptions(SystemAdjustOptions.SystemAdjustOptions):    
     """class extending the basic interface of SystemAdjusts so specific Linux options are included"""
     
     def __init__(self , is_full_disk = True):
         """Constructor. Inits adjust options with default values"""
         super(LinuxAdjustOptions,self).__init__()
         random.seed()
         if is_full_disk == False:
            self.__systemPartStart = long(0x0100000);
            self.__newMbr = int(random.randint(1, 0x0FFFFFFF))
         else:
            self.__systemPartStart = long(0);
            self.__newMbr = 0
         
         
         # stop some services
         self.__stopServices = list()
         # reconfig ips settings
         self.__reconfigIps = False
         
         
     def __getValue(self , config , key , default = None):
         """auxillary to read data from dict-like config with no exceptions"""
         try:
            return config[key]
         except KeyError:
            return default

     def loadConfig(self , config):
         """
         Loads the adjust specific data from config. If no data specified defaults are used

         Args:
            config: dict - any dict-like config supporting [] operator
         """
         super(LinuxAdjustOptions,self).loadConfig(config)
         

         #TODO: load ini config setting these values
         #get config class from the service
         return 

     

     def getNewMbrId(self):
        """get new mbr id for the system drive"""
        return  self.__newMbr
    
     def setNewMbrId(self , newMbr):
        """sets new mbr id for the system drive"""
        self.__newMbr = newMbr 

     def getNewSysPartStart(self):
        """gets the new system partition start"""
        return self.__systemPartStart
    
     def setNewSysPartStart(self , newPart):
         """sets the new system partition start"""
         self.__systemPartStart = newPart