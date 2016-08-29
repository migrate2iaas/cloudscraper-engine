"""
onAppConfigs
~~~~~~~~~~~~~~~~~

This module provides onAppConfigs class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import datetime
import threading

import SystemAdjustOptions
import CloudConfig
import MigrateConfig
import UploadManifest

import S3UploadChannel


import onAppInstanceGenerator

import time
import os


class onAppCloudOptions(CloudConfig.CloudConfig):
    

    def __init__(self, s3bucket, s3user, s3password, s3region, onapp_endpoint, onapp_login, onapp_password,
                 onapp_datastore_id, onapp_target_account=None, onapp_port=80, preset_ip=None,
                 minipad_image_name="", minipad_vm_id=None, vmbuild_timeout_sec=120*60, wintemplate_size=20, network=None,
                 s3custom=False, vm_boot_timeout=120, manifest_path=None, increment_depth=1, use_dr=False, db_write_cache_size=20,
                 os_override=None, extra_vm_parms_json = None):
        """
        Constructor
        """
        self.__s3bucket = s3bucket
        self.__s3user = s3user
        self.__s3password = s3password
        self.__s3region = s3region
        self.__onapp_endpoint = onapp_endpoint
        self.__onapp_login = onapp_login
        self.__onapp_password = onapp_password
        self.__onapp_datastore_id = onapp_datastore_id
        self.__onapp_target_account = onapp_target_account
        self.__onapp_port = onapp_port
        self.__preset_ip = preset_ip
        self.__minipad_image_name = minipad_image_name
        self.__minipad_vm_id = minipad_vm_id
        self.__custom_host = s3custom
        self.__keynamePrefix = "{0}/".format(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M"))
        self.__diskType = "RAW"
        self.__chunkSize = 10*1024*1024
        self.__instanceFactory = None
        self.__vmBuildTimeout = vmbuild_timeout_sec 
        self.__winTemplateDiskSize = wintemplate_size
        self.__vmBootTimeout = vm_boot_timeout
        # DR params
        self.__manifest_path = manifest_path
        self.__increment_depth = increment_depth
        self.__use_dr = use_dr
        self.__db_write_cache_size = db_write_cache_size
        self.__os_override = os_override
        self.__network = network
        self.__extra_vm_parms_json = extra_vm_parms_json

        #generate instance factory to test the connection
        self.__instanceFactory = onAppInstanceGenerator.onAppInstanceGenerator(self.__onapp_endpoint , self.__onapp_login , self.__onapp_password , self.__onapp_datastore_id, self.__onapp_target_account, \
            self.__onapp_port, self.__preset_ip , self.__minipad_image_name , self.__minipad_vm_id , vmbuild_timeout = self.__vmBuildTimeout , win_template_disk_size = self.__winTemplateDiskSize , 
                                                                               vm_boot_timeout = self.__vmBootTimeout , os_override = os_override, network = self.__network , extra_vm_parms_json = self.__extra_vm_parms_json)


        super(onAppCloudOptions, self).__init__()

        
        
    def generateUploadChannel(
            self, targetsize, targetname=None, targetid=None, resume=False, imagesize=0, volname=None):
        """
        Generates new upload channel

        Args:
            targetsize: long - target cloud disk size in bytes
            targetname: str - arbitrary description to mark the disk after migration (ignored)
            targetid: str - a cloud-defined path describing the upload (blob-name for Azure)
            resume: Boolean - to recreate disk representation (False) or to reupload (True)
            imagesize: long - image file size in bytes
        """
        custom = False
        if self.__custom_host:
            custom = True

        manifest = UploadManifest.ImageManifestDatabase(
            UploadManifest.ImageDictionaryManifest, self.__manifest_path, None, threading.Lock(),
            increment_depth=self.__increment_depth, db_write_cache_size=self.__db_write_cache_size,
            use_dr=self.__use_dr, resume=resume, volname=volname, target_id=targetid)

        return S3UploadChannel.S3UploadChannel(
            self.__s3bucket, self.__s3user, self.__s3password, targetsize, self.__s3region,
            targetid or self.__keynamePrefix, self.__diskType, chunksize=self.__chunkSize,
            walrus=custom, walrus_path="", walrus_port=443, make_link_public=True, manifest=manifest)

    def generateInstanceFactory(self):
        #generate signleton
        return self.__instanceFactory

    def getCloudStorage(self):
        return self.__s3bucket

    def getCloudUser(self):
        return self.__s3user
    
    def getCloudPass(self):
        return self.__s3password
    
    def getTargetCloud(self):
        return "onApp"

    def getArch(self):
        return "x86_64"

    def getZone(self):
        """returns """
        return self.__affinityGroup

    def getRegion(self):
        return self.__onapp_endpoint

    def getSecurity(self):
        return ""

    def getUploadChunkSize(self):
        return self.__chunkSize

    def getInstanceType(self):
        return ""

    def getServerName(self):
        return ""

    def getSubnet(self):
        return "" 

class onAppMigrateConfig(MigrateConfig.MigrateConfig):

    #TODO: make docs
    # images is list of VolumeMigrateConfig
    def __init__(self, images , media_factory , source_arch , imagetype):
        super(onAppMigrateConfig, self).__init__(images, media_factory)

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

    def insertXen(self):
        return False

