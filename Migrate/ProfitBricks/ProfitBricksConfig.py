"""
ProfitBricksConfig
~~~~~~~~~~~~~~~~~

This module provides ProfitBricksConfig class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback



import SystemAdjustOptions
import CloudConfig
import MigrateConfig
import FtpUploadChannel

import time
import os



class ProfitBricksCloudOptions(CloudConfig.CloudConfig):
    """Options to set in EH cloud"""

    # avoid means which servers\disks to avoid, the option is rarely used
    def __init__(self, user , password , datacenterlink , imagetype, chunksize=1*1024*1024):
        self.__user = user
        self.__pass = password
        self.__region = datacenterlink
        self.__chunkSize = chunksize
        self.__imagetype = imagetype
        
        super(ProfitBricksCloudOptions, self).__init__()

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

        if str(targetid).startswith("hdd-images") == False:
            targetid = "hdd-images/" + targetid 
        if str(targetid).endswith(self.__imagetype) == False:
            targetid = targetid + "." + self.__imagetype
        return FtpUploadChannel.FtpUploadChannel(filepath=targetid, user=self.__user , password=self.__pass , hostname=self.__region , resume=resume)
     
    def generateInstanceFactory(self):
        return None
        #return ProfitBricksInstanceGenerator.ProfitBricksInstanceGenerator(self.__user, self.__pass , self.__region)    
     
    def getCloudStorage(self):
        return ''

    def getCloudUser(self):
        return self.__user
    
    def getCloudPass(self):
        return  self.__pass
    
    def getNewSystemSize(self):
        return 0

    def getTargetCloud(self):
        return "ProfitBricks"

    def getArch(self):
        return ""

    #returns avoid
    def getZone(self):
        return ""

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

class ProfitBricksMigrateConfig(MigrateConfig.MigrateConfig):
    """Options on how to migrate source system to EH"""
    
    def __init__(self, images , media_factory , source_arch , image_type):
        super(ProfitBricksMigrateConfig, self).__init__(images,media_factory)
        
        self.__imageArch = source_arch
        #do we need few images? dunno...
        self.__imageType = image_type

    def getSourceOs(self):
        return "local"


    def getSourceArch(self):
        raise self.__imageArch

    def getImageType(self):
        return self.__imageType
    
    def getImagePlacement(self):
        return "local"

    def getSystemConfig(self):
        #TODO: really , dunno what should be palced here. should make some umls to see what needed to be changed
        return None

    def insertVirtIo(self):
        return True

