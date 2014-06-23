# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import random
import SystemAdjustOptions
import os

class WindowsSystemAdjustOptions(SystemAdjustOptions.SystemAdjustOptions):    
     """class extending the basic interface of SystemAdjusts so specific Windows options are included"""
     
     def __init__(self, detect_hal = True , enable_hyperv = True):
         """Constructor. Inits adjust options with default values"""
         super(WindowsSystemAdjustOptions,self).__init__()
         random.seed()
         self.__systemPartStart = long(0x0100000);
         self.__newMbr = int(random.randint(1, 0x0FFFFFFF))
         self.__prebuiltBcdPath = "..\\resources\\boot\\win\\BCD_MBR"
         self.__detectHal = detect_hal
         self.__fixRdp = True
         self.__rdpPort = 3389
         self.__turnHyperV = enable_hyperv
         self.__adjustPageFile = False
         self.__adjustTcpIp = True
         
         originalwindir = os.environ['windir']
         windir = originalwindir.split(":\\")[-1] #get substring after X:\
         self.__systemHiveFilePath = windir+"\\system32\\config\\system"
         self.__softwareHiveFilePath = windir+"\\system32\\config\\SOFTWARE"
         
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
         self.__turnHyperV = self.__getValue(config , 'turn-hyperv' , self.__turnHyperV)
         self.__systemHiveFilePath = self.__getValue(config , 'system-hive-file' , self.__systemHiveFilePath)
         self.__softwareHiveFilePath = self.__getValue(config , 'software-hive-file' , self.__softwareHiveFilePath)
         self.__adjustPageFile = self.__getValue(config , 'adjust-pagefile' , self.__adjustPageFile)
         self.__adjustTcpIp = self.__getValue(config , 'adjust-tcpip' , self.__adjustTcpIp)

         #TODO: load ini config setting these values
         #get config class from the service
         return 

     
     #Windows special methods

     def detectHal(self):
         """if HAL should be automatically detected. Affects BCD manipulations for Win2008+"""
         return self.__detectHal

     def getPregeneratedBcdHivePath(self):
        """gets precretead bcd"""
        return  self.__prebuiltBcdPath

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

     # gets new mount points
     def getMountPoints(self):
         """gets extra predefined mount points. NOT IMPLEMENTED"""
         return

     def fixRDP(self):
         """Returns True or False whether to allow RDP connection for this host"""
         return self.__fixRdp

     def rdpPort(self):
         """returns rdp TCP port to use"""
         return self.__rdpPort

     def turnOnHyperV(self):
         """Turn if internal hyperV bus should be enabled by default"""
         return self.__turnHyperV

     def systemHivePath(self):
         """Returns the file path relative to system drive where the system registry hive is located. Could be used to set alternative initial registry"""
         return self.__systemHiveFilePath

     def softwareHivePath(self):
         """Returns the file path relative to system drive where the system registry hive is located. Could be used to set alternative initial registry"""
         return self.__softwareHiveFilePath

     def adjustPageFile(self):
         """returns if to adjust pagefile so it will reside on system volume"""
         return self.__adjustPageFile

     def adjustTcpIp(self):
         """returns if system tcpip options including dhcp has to be adjusted to boot on new DHCP enabled network"""
         return self.__adjustTcpIp