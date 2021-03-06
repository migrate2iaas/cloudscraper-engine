# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import SystemAdjustOptions
import CloudConfig
import MigrateConfig
import EHUploadChannel
import EHInstanceGenerator

import time
import os



class EHCloudOptions(CloudConfig.CloudConfig):
    """Options to set in EH cloud"""

    # avoid means which servers\disks to avoid, the option is rarely used
    def __init__(self, user , password , newsize , arch , region , avoid , chunksize = 4096*4096):
        self.__user = user
        self.__pass = password
        self.__newSysSize = newsize
        self.__arch = arch
        self.__zone = avoid
        self.__region = region
        self.__securityGroup = ''
        self.__chunkSize = chunksize
        
        super(EHCloudOptions, self).__init__()

    def generateUploadChannel(self , targetsize , targetname = None, targetid = None , resume = False , imagesize = 0 , volname="system"):  
        """
        Generates new upload channel

        Args:
            targetsize: long - target cloud disk size in bytes
            targetname: str - arbitrary description to mark the disk after migration (User-visible name for a disk)
            targetid: str - a cloud-defined path describing the upload (ElasticHosts disk UUID)
            resume: Boolean - to recreate disk representation (False) or to reupload (True)
            imagesize: long - image file size in bytes (ignored)
        """ 
        return EHUploadChannel.EHUploadChannel(targetid, self.__user , self.__pass , targetsize, self.__region , targetname , self , resume)
     
    def generateInstanceFactory(self):
        return EHInstanceGenerator.EHInstanceGenerator(self.__user, self.__pass , self.__region)    
     
    def getCloudStorage(self):
        return ''

    def getCloudUser(self):
        return self.__user
    
    def getCloudPass(self):
        return  self.__pass

    def getTargetCloud(self):
        return "ElasticHosts"

    def getArch(self):
        return self.__arch

    #returns avoid
    def getZone(self):
        return self.__zone

    def getRegion(self):
        return self.__region

    def getSecurity(self):
        return ''

    def getUploadChunkSize(self):
        return self.__chunkSize

    def getServerName(self):
        return ""

    def getSubnet(self):
        return ""

class EHMigrateConfig(MigrateConfig.MigrateConfig):
    """Options on how to migrate source system to EH"""
    
    def __init__(self, images , media_factory , source_arch , image_type):
        super(EHMigrateConfig, self).__init__(images,media_factory)
        
        self.__imageArch = source_arch
        #do we need few images? dunno...
        self.__imageType = image_type

    def getSourceOs(self):
        #should make it more flexible
        return self.getHostOs()


    def getSourceArch(self):
        raise self.__imageArch

    def getImageType(self):
        return self.__imageType
    
    def getImagePlacement(self):
        return "local"

    def getSystemConfig(self):
        #TODO: really , dunno what should be palced here. should make some umls to see what needed to be changed
        return None
    