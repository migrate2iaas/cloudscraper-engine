# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import SystemAdjustOptions
import CloudConfig
import MigrateConfig
import S3UploadChannel
import EC2InstanceGenerator

import time
import os


class AmazonCloudOptions(CloudConfig.CloudConfig):
    
    def __init__(self, bucket , user , password , newsize , arch , zone , region , machinename , securityid='' , instancetype='m1.small' , chunksize = 10*1024*1024 , disktype='VHD' , keyname_prefix = '' , vpc = ""):
        """inits with options"""
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
        self.__instanceType = instancetype
        self.__machineName = machinename
        self.__diskType = disktype
        self.__keynamePrefix = keyname_prefix
        self.__vpc = vpc
        #TODO: more amazon-specfiic configs needed
    
    def generateUploadChannel(self , targetsize , targetname = None, targetid = None , resume = False , imagesize = 0):   
        return S3UploadChannel.S3UploadChannel(self.__bucket , self.__user , self.__pass , targetsize, self.__region , targetid or self.__keynamePrefix , self.__diskType , resume_upload = resume , chunksize = self.__chunkSize)
         
    def generateInstanceFactory(self):
        """returns object of InstanceFactory type to create servers from uploaded images"""
        return EC2InstanceGenerator.EC2InstanceGenerator(self.__region)

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

    def getInstanceType(self):
        return self.__instanceType

    def getServerName(self):
        return  self.__machineName

    def getSubnet(self):
        return self.__vpc

class AmazonMigrateConfig(MigrateConfig.MigrateConfig):

    #TODO: make docs
    # images is list of VolumeMigrateConfig
    def __init__(self, images , media_factory , source_arch , imagetype):
        super(AmazonMigrateConfig, self).__init__(images, media_factory)

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
