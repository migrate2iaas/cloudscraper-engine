"""
AzureConfigs
~~~~~~~~~~~~~~~~~

This module provides CloudSigma configuration classes
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
import AzureUploadChannel
import AzureInstanceGenerator

import time
import os


class AzureCloudOptions(CloudConfig.CloudConfig):
    
    def __init__(self,  account , storage_key, container_name , region, subscription = "" , certpath = "" , instance_type="small", chunksize = 1024*1024):
        super(AzureCloudOptions, self).__init__()
        self.__storageAccount = account
        self.__storageKey = storage_key
        self.__containerName = container_name
        self.__region = region
        self.__instanceType = instance_type
        self.__chunkSize = chunksize
        self.__certPath = certpath
        self.__subscription = subscription
        #self.__machinename = machinename
        
    def generateUploadChannel(self , targetsize , targetname = None, targetid = None , resume = False):   
        if targetname:
            if targetname.endswith(".vhd") == False:
                targetname = targetname + ".vhd"
        return AzureUploadChannel.AzureUploadChannel(self.__storageAccount ,  self.__storageKey , targetsize , self.__containerName, targetname, resume , self.__chunkSize)

    def generateInstanceFactory(self):
        if self.__subscription and self.__certPath:
            return AzureInstanceGenerator.AzureInstanceGenerator(self.__subscription , self.__certPath)
        else:
            logging.warning("! The system disk image is uploaded to your Azure Storage but VM disk wasn't created since no management certificate was specified");
            logging.info(">>>>>>>>>>>>>>>> Please, create VM disk via Disks menu of Virtual Machine tab in Windows Azure management console");
            return None

    def getCloudStorage(self):
        return self.__containerName

    def getCloudUser(self):
        return self.__storageAccount
    
    def getCloudPass(self):
        return self.__storageKey
    
    def getTargetCloud(self):
        return "Azure"

    def getArch(self):
        return "x86_64"

    def getZone(self):
        return ""

    def getRegion(self):
        return self.__region

    def getSecurity(self):
        return ""

    def getUploadChunkSize(self):
        return self.__chunkSize

    def getInstanceType(self):
        return self.__instanceType

    def getServerName(self):
        return ""

    def getSubnet(self):
        return ""

class AzureMigrateConfig(MigrateConfig.MigrateConfig):

    #TODO: make docs
    # images is list of VolumeMigrateConfig
    def __init__(self, images , media_factory , source_arch , imagetype):
        super(AzureMigrateConfig, self).__init__(images, media_factory)

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

    def getSystemConfig(self):
        #TODO: really , dunno what should be palced here. should make some umls to see what needed to be changed
        return None



