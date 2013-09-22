# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

class SystemAdjustOptions(object):
    """Basic class defining system adjusts loaded from pre-generated config file"""

    diskUnknown = 0
    diskScsi = 1
    diskAta = 2

    def __init__(self):
        self.__diskType = self.diskAta
        self.__servicePath = None
        self.__serviceConfigPath = None
        self.__installService = False

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
        self.__serviceConfigPath = self.__getValue(config , 'service-config-path' , self.__serviceConfigPath)
        self.__servicePath = self.__getValue(config , 'service-bin-path' , self.__servicePath)
        self.__installService = self.__getValue(config , 'service-install' , self.__installService)
        self.__diskType = self.__getValue(config , 'disk-type' , self.__diskType)
        return 

    def getSysDiskType(self):
        return self.__diskType 

    def setSysDiskType(self, diskType):
        self.__diskType = diskType

    
    def installNotifyService(self):
         """returns True or False whether the notification service should be installed"""
         return self.__installService

    def getNotificationServicePath(self):
         """returns absolute path to the notification service executable"""
         return self.__servicePath

    def getNotificationServiceConfigPath(self):
         """returns absolute path to the notification service config"""
         return self.__serviceConfigPath
