"""
onAppConfigs
~~~~~~~~~~~~~~~~~

This module provides onAppConfigs class
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

import OpenStackUploadChannel


import OpenStackInstanceGenerator

import time
import os


class OpenStackCloudOptions(CloudConfig.CloudConfig):
    
    def __init__(self, server_url, username , tennant_name, password, network_uuid = None , disk_format="vhd", container_format="bare"):
        """
        Constructor
        """
        self.__server = server_url
        self.__username = username
        self.__tennant = tennant_name
        self.__password = password
        self.__chunkSize = 64*1024
        self.__disk_format = str(disk_format).lower()
        self.__network = network_uuid
        self.__container = container_format
        super(OpenStackCloudOptions, self).__init__()

        
        
    def generateUploadChannel(self , targetsize , targetname = None, targetid = None , resume = False , imagesize = 0):
        """
        Generates new upload channel

        Args:
            targetsize: long - target cloud disk size in bytes
            targetname: str - arbitrary description to mark the disk after migration (ignored)
            targetid: str - a cloud-defined path describing the upload (blob-name for Azure)
            resume: Boolean - to recreate disk representation (False) or to reupload (True)
            imagesize: long - image file size in bytes
        """

        return OpenStackUploadChannel.OpenStackUploadChannel(imagesize , self.__server , self.__tennant , self.__username , self.__password , self.__disk_format, targetname, resume, self.__chunkSize , self.__container);

    def generateInstanceFactory(self):
        return OpenStackInstanceGenerator.OpenStackInstanceGenerator(self.__server , self.__tennant , self.__username , self.__password)

    def getCloudStorage(self):
        return ""

    def getCloudUser(self):
        return self.__username
    
    def getCloudPass(self):
        return self.__password
    
    def getTargetCloud(self):
        return "OpenStack"

    def getArch(self):
        return "x86_64"

    def getZone(self):
        """returns """
        return self.__tennant

    def getRegion(self):
        return self.__server

    def getSecurity(self):
        return ""

    def getUploadChunkSize(self):
        return self.__chunkSize

    def getInstanceType(self):
        return ""

    def getServerName(self):
        return ""

    def getSubnet(self):
        return self.__network

class OpenStackMigrateConfig(MigrateConfig.MigrateConfig):

    #TODO: make docs
    # images is list of VolumeMigrateConfig
    def __init__(self, images , media_factory , source_arch , imagetype):
        super(OpenStackMigrateConfig, self).__init__(images, media_factory)

        self.__imageArch = source_arch
        #do we need few images? dunno...
        self.__imageType = imagetype

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

