# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import random
import SystemAdjustOptions
import os

class WindowsSystemAdjustOptions(SystemAdjustOptions.SystemAdjustOptions):    
     """class extending the basic interface of SystemAdjusts so specific Windows options are included"""
     
     def __init__(self, detectHal = True):
         """Constructor. Inits adjust options with default values"""
         super(WindowsSystemAdjustOptions,self).__init__()
         random.seed()
         self.__systemPartStart = long(0x0100000);
         self.__newMbr = int(random.randint(1, 0x0FFFFFFF))
         self.__prebuiltBcdPath = "..\\resources\\boot\\win\\BCD_MBR"
         self.__detectHal = detectHal
         self.__fixRdp = True
         self.__rdpPort = 3389
         
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
         super(WindowsSystemAdjustOptions,self).loadConfig(config)
         
         self.__prebuiltBcdPath = self.__getValue(config , 'bcd-path' , self.__prebuiltBcdPath)
         self.__newMbr = self.__getValue(config , 'mbr-id' , self.__newMbr)
         self.__systemPartStart = self.__getValue(config , 'syspart-start' , self.__systemPartStart)
         self.__fixRdp = self.__getValue(config , 'fix-rdp' , self.__fixRdp)
         self.__detectHal = self.__getValue(config , 'detect-hal' , self.__detectHal)
         self.__rdpPort = self.__getValue(config , 'rdp-port' , self.__rdpPort)

         #TODO: load ini config setting these values
         #get config class from the service
         return 

     
     #Windows special methods

     def detectHal(self):
         return self.__detectHal

     #
     def getPregeneratedBcdHivePath(self):
        return  self.__prebuiltBcdPath

     #
     def getNewMbrId(self):
        return  self.__newMbr
    
     #
     def setNewMbrId(self , newMbr):
        self.__newMbr = newMbr 

     #  
     def getNewSysPartStart(self):
        return self.__systemPartStart
    
     #
     def setNewSysPartStart(self , newPart):
         self.__systemPartStart = newPart

     # gets new mount points
     def getMountPoints(self):
         return

     def fixRDP(self):
         """Returns True or False whether to allow RDP connection for this host"""
         return self.__fixRdp

     def rdpPort(self):
         """returns rdp TCP port to use"""
         return self.__rdpPort
