


import SystemAdjustOptions
import CloudConfig
import MigrateConfig

import time

class AmazonAdjustOptions(SystemAdjustOptions.SystemAdjustOptions):

    def __init__(self):
        super(AmazonAdjustOptions, self).__init__()
        # we emulate hyper-v import so the disk should be ata-based
        self.setSysDiskType(SystemAdjustOptions.SystemAdjustOptions.diskAta)
        #TODO: improve the adjust class: we need more on rdp and stuff



class AmazonCloudOptions(CloudConfig.CloudConfig):
    
    def __init__(self, bucket , user , password , newsize , arch , zone , region):
        super(AmazonCloudOptions, self).__init__()
        self.__bucket = bucket
        self.__user = user
        self.__pass = password
        self.__newSysSize = newsize
        self.__arch = arch
        self.__zone = zone
        self.__region = region
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
        return  self.__zone

    def getRegion(self):
        return  self.__region

class AmazonMigrateConfig(MigrateConfig.MigrateConfig):

    def __init__(self, imagepath , imagesize , source_arch ,  imagetype='VHD'):
        super(AmazonMigrateConfig, self).__init__()
        
        self.__imageType = imagetype
        self.__imageSize = imagesize
        self.__imageArch = source_arch
        self.__imagePath = imagepath

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
        return self.__imagePath 

    def getSystemImageSize(self):
        return self.__imageSize

    def getSystemConfig(self):
        #TODO: dunno what should be placed here
        return None
