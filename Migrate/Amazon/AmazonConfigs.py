


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

    #TODO: make docs
    # images is list of tuples (volume_device_name:string, volume_image_path:string , image_size:long iin bytes)
    def __init__(self, images , source_arch ,  imagetype='VHD'):
        super(AmazonMigrateConfig, self).__init__()
        
        self.__dataVolumes = list()
        self.__imageSize = None
        self.__imagePath = None

        #TODO: make cross-system
        for (volume_device_name, volume_image_path , image_size) in images:
            originalwindir = os.environ['windir']
            windrive = originalwindir.split("\\")[0] #get C: substring
            if windrive in volume_device_name:
                self.__imageSize = image_size
                self.__imagePath = volume_image_path         
            else:
                self.__dataVolumes.append( (volume_device_name, volume_image_path , image_size) )

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
        return self.__imagePath 

    def getSystemImageSize(self):
        return self.__imageSize

    def getSystemConfig(self):
        #TODO: dunno what should be placed here
        return None
    
    # gets list of string tuples (volume_device_name, volume_image_path , image_size)
    def getDataVolumes(self):
        return self.__dataVolumes