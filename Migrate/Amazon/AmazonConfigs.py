# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import SystemAdjustOptions
import CloudConfig
import MigrateConfig

import time
import os

class AmazonAdjustOptions(SystemAdjustOptions.SystemAdjustOptions):

    def __init__(self):
        super(AmazonAdjustOptions, self).__init__()
        # we emulate hyper-v import so the disk should be ata-based
        self.setSysDiskType(SystemAdjustOptions.SystemAdjustOptions.diskAta)
        #TODO: improve the adjust class: we need more on rdp and stuff



class AmazonCloudOptions(CloudConfig.CloudConfig):
    
    def __init__(self, bucket , user , password , newsize , arch , zone , region , securityid='' , chunksize = 10*1024*1024):
        super(AmazonCloudOptions, self).__init__()
        self.__bucket = bucket
        self.__user = user
        self.__pass = password
        self.__newSysSize = newsize
        self.__arch = arch
        self.__zone = zone
        self.__region = region
        self.__securityGroup = securityid
        self.__chunkSize = chunksize
        #TODO: more amazon-specfiic configs needed
      
    def getCloudStorage(self):
        return self.__bucket

    def getCloudUser(self):
        return self.__user
    
    def getCloudPass(self):
        return  self.__pass
    
    def getNewSystemSize(self):
        return self.__newSysSize

    def getTargetCloud(self):
        return "EC2"

    def getArch(self):
        return self.__arch

    def getZone(self):
        return self.__zone

    def getRegion(self):
        return self.__region

    def getSecurity(self):
        return self.__securityGroup

    def getUploadChunkSize(self):
        return self.__chunkSize

class AmazonMigrateConfig(MigrateConfig.MigrateConfig):

    #TODO: make docs
    # images is list of VolumeMigrateConfig
    def __init__(self, images , source_arch ,  imagetype='VHD'):
        super(AmazonMigrateConfig, self).__init__()
        
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
        return "local"

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