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
    
    def __init__(self, server_url, username , tennant_name, password, network_name = None , disk_format="vhd", container_format="bare", flavor=None, ip_pool_name=None,\
        swift_server_url = None , swift_tennant_name = None , swift_username = None , swift_password = None , swift_container="cloudscraper-upload" , compression=0, chunksize=10*1024*1024,
        use_new_channel=False, resume_file_path=None , ignore_etag=False):
        """
        Constructor
        """
        self.__server = server_url
        self.__username = username
        self.__tennant = tennant_name
        self.__password = password
        self.__chunkSize = chunksize
        self.__disk_format = str(disk_format).lower()
        self.__network = network_name
        self.__container_format = container_format
        self.__instanceFlavor = flavor
        self.__publicIpPool = ip_pool_name
        self.__useNewChannel = use_new_channel
        self.__resumeFilePath = resume_file_path

        self.__swiftUrl = swift_server_url 
        self.__swiftTennant = swift_tennant_name 
        self.__swiftUsername = swift_username
        self.__swiftPassword = swift_password
        self.__swiftContainer = swift_container
        self.__ignoreEtag = ignore_etag
        
        if compression: # in case compression is int 
            self.__compression = True
        else:
            self.__compression = False

        super(OpenStackCloudOptions, self).__init__()

        
        
    def generateUploadChannel(self , targetsize , targetname = None, targetid = None , resume = False , imagesize = 0):
        """
        Generates new upload channel

        Args:
            targetsize: long - target cloud disk size in bytes
            targetname: str - arbitrary description to mark the disk after migration (ignored)
            targetid: str - a cloud-defined path describing the upload 
            resume: Boolean - to recreate disk representation (False) or to reupload (True)
            imagesize: long - image file size in bytes
        """

        return OpenStackUploadChannel.OpenStackUploadChannel(
            imagesize,
            self.__server,
            self.__tennant,
            self.__username,
            self.__password,
            self.__disk_format,
            targetname,
            resume,
            self.__chunkSize,
            self.__container_format,
            swift_server_url=self.__swiftUrl,
            swift_tennant_name=self.__swiftTennant,
            swift_username=self.__swiftUsername,
            swift_password=self.__swiftPassword,
            disk_name=targetname,
            container_name=self.__swiftContainer,
            compression=self.__compression,
            use_new_channel=self.__useNewChannel,
            resume_file_path=self.__resumeFilePath,
            ignore_etag = self.__ignoreEtag)

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
        # pool is very alike availability zone. Not sure whether they are the same or just interrelated in the sys architectures on hand
        return self.__publicIpPool

    def getRegion(self):
        return self.__server

    def getSecurity(self):
        return ""

    def getUploadChunkSize(self):
        return self.__chunkSize

    def getInstanceType(self):
        return self.__instanceFlavor

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

