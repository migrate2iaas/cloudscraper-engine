# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import time
import os
import random


class ConfigAccessor(object):
    """base class to save the config value"""

    def saveValue(self , valuename , value):
        return 
    def getValue(self , valuename , value):
        return


#TODO: implement the config saver
# add save to config here
# so the configurer adds 
class VolumeMigrateConfig(object):
    """ base class for volume migration parms """

    def __init__(self):
        # mbr_id value used in two cases
        # first is Migrator->createTransferTarget. Function create disk image
        # second is WindowsBackupAdjust->adjustSystemHive. Function setup HKLM\System\MountedDevices
        self.__mbr_id = int(random.randint(1, 0x0FFFFFFF))

    def getMbrId(self):
        return self.__mbr_id

    def getImagePath(self):
        raise NotImplementedError

    def getUploadPath(self):
        raise NotImplementedError

    def getUploadId(self):
        raise NotImplementedError

    def getImageSize(self):
        raise NotImplementedError

    def getVolumePath(self):
        raise NotImplementedError

    # returns iterable of excluded dirs
    # NOTE: some more sophisticated exclude system is to be designed
    # if this mechanism is to be really used
    def getExcludedDirs(self):
        raise NotImplementedError

    def setUploadPath(self, path):
        raise NotImplementedError

    def setUploadId(self , uploadid):
        raise NotImplementedError

    def setImagePath(self , imagepath):
        raise NotImplementedError

    def setImageSize(self , size):
        raise NotImplementedError
   
    def saveConfig(self):
        raise NotImplementedError

    def isSystem(self):
        raise NotImplementedError

    def setSystem(self , system_flag):
        raise NotImplementedError

class MigrateConfig(object):
    """ base class for the migration config"""

    def __init__(self , images, media_factory):
        self.__dataVolumes = list()
        self.__systemVolumeConfig = None
        self.__mediaFactory = media_factory

        #TODO: make cross-system
        for config in images:
            # we emulate 'windir' for linux too
            originalwindir = os.environ['windir']
            windrive = originalwindir.split("\\")[0] #get C: substring
            #TODO: should remove this system volume auto-detection and rely on isSystem() flag solely
            if windrive in config.getVolumePath() or config.isSystem():
                self.__systemVolumeConfig = config
            else:
                #TODO: make migration config for each volume not to have all this stuff
                self.__dataVolumes.append( config )
        
    def getImageFactory(self):
        return self.__mediaFactory

    #local system or the pre-created image. No fixups on images are currently supported
    def getSourceOs(self):
        raise NotImplementedError

    def getHostOs(self):
        if os.name == 'nt':
            return 'Windows'
        else:
            return 'Linux'


    def getImageType(self):
        raise NotImplementedError
    
    def getImagePlacement(self):
        raise NotImplementedError

    def getSystemConfig(self):
        raise NotImplementedError

    
    def getSystemImagePath(self):
        return self.__systemVolumeConfig.getImagePath()

    def getSystemImageSize(self):
        return self.__systemVolumeConfig.getImageSize()

    def getSystemVolumeConfig(self):
        return self.__systemVolumeConfig
    
    # gets list of VolumeMigrateConfig
    def getDataVolumes(self):
        return self.__dataVolumes
     
    def insertVirtIo(self):
        return False

    def insertXen(self):
        return False