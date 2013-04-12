# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

class SystemAdjustOptions(object):
    """Abstract class defining system adjusts laoded from pre-generated config file"""

    diskUnknown = 0
    diskScsi = 1
    diskAta = 2

    def __init__(self):
        self.__diskType = self.diskUnknown
        return

    def loadConfig(self , adjustOptionConfig):
         return 

    def getSysDiskType(self):
        return self.__diskType 

    def setSysDiskType(self, diskType):
        self.__diskType = diskType
