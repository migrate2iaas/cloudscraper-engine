# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import SystemAdjustOptions
import CloudConfig
import MigrateConfig

import time
import os

class EHAdjustOptions(SystemAdjustOptions.SystemAdjustOptions):

    def __init__(self):
        super(EHAdjustOptions, self).__init__()
        # EH support lots of disk types. We use Disk Ata but diskUnknown could be used aswell
        self.setSysDiskType(SystemAdjustOptions.SystemAdjustOptions.diskAta)
        #TODO: improve the adjust class: we need more on rdp and stuff



class EHCloudOptions(CloudConfig.CloudConfig):
    
    def __init__(self, user , password , newsize , arch , region ):
        super(EHCloudOptions, self).__init__()
        self.__user = user
        self.__pass = password
        self.__newSysSize = newsize
        self.__arch = arch
        self.__zone = ''
        self.__region = region
        self.__securityGroup = ''
      
    def getCloudStorage(self):
        return ''

    def getCloudUser(self):
        return self.__user
    
    def getCloudPass(self):
        return  self.__pass
    
    def getNewSystemSize(self):
        return self.__newSysSize

    def getTargetCloud(self):
        return "ElasticHosts"

    def getArch(self):
        return self.__arch

    def getZone(self):
        return ''

    def getRegion(self):
        return self.__region

    def getSecurity(self):
        return ''

class EHMigrateConfig(MigrateConfig.MigrateConfig):

    #TODO: make docs
    # images is list of VolumeMigrateConfig
    def __init__(self, images , source_arch , imageplacement = 'local', imagetype='raw.gz'):
        super(EHMigrateConfig, self).__init__()
        
        self.__dataVolumes = list()
        self.__systemVolumeConfig = None

        #TODO: make cross-system
        for config in images:
            originalwindir = os.environ['windir']
            windrive = originalwindir.split("\\")[0] #get C: substring
            if windrive in config.getVolumePath():
                self.__systemVolumeConfig = config
            else:
                #TODO: make migration config for each volume not to have all this stuff
                self.__dataVolumes.append( config )

        self.__imageArch = source_arch
        #do we need few images? dunno...
        self.__imageType = imagetype
        self.__imagePlacement = imageplacement


    def getSourceOs(self):
        return "local"

    def getHostOs(self):
        #TODO: should be analyzed by the system
        return "Windows"

    def getSourceArch(self):
        raise self.__imageArch

    def getImageType(self):
        return self.__imageType
    
    def getImagePlacement(self):
        return self.__imagePlacement

    def getSystemImagePath(self):
        return self.__systemVolumeConfig.getImagePath()

    def getSystemImageSize(self):
        return self.__systemVolumeConfig.getImageSize()

    def getSystemVolumeConfig(self):
        return self.__systemVolumeConfig

    def getSystemConfig(self):
        #TODO: really , dunno what should be palced here. should make some umls to see what needed to be changed
        return None
    
    # gets list of VolumeMigrateConfig
    def getDataVolumes(self):
        return self.__dataVolumes