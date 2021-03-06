"""
onAppConfigs
~~~~~~~~~~~~~~~~~

This module provides OpenStackConfigs class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import threading

import CloudConfig
import MigrateConfig

import OpenStackUploadChannel
import GlanceUploadChannel
import UploadManifest

import OpenStackInstanceGenerator


class OpenStackCloudOptions(CloudConfig.CloudConfig):
    

    def __init__(
            self, server_url, username, tennant_name, password, network_name=None, disk_format="vhd",
            container_format="bare", flavor=None, ip_pool_name=None, swift_server_url=None, swift_tennant_name=None,
            swift_username=None, swift_password=None, swift_container="cloudscraper-upload", compression=0, swift_max_segments=0, swift_use_slo=True,
            chunksize=10*1024*1024, 
            use_new_channel=False, 
            manifest_path=None, 
            increment_depth=1, 
            ignore_etag=False, 
            glance_only=False,
            ignore_ssl_cert = False,
            private_container = False,
            use_dr=False ,  
            db_write_cache_size=20,
            availability_zone = "",
            multithread_queue = False):
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
        self.__manifestPath = manifest_path
        self.__increment_depth = increment_depth
        self.__swiftUrl = swift_server_url
        self.__swiftTennant = swift_tennant_name 
        self.__swiftUsername = swift_username
        self.__swiftPassword = swift_password
        self.__swiftContainer = swift_container
        self.__ignoreEtag = ignore_etag
        self.__glanceOnly = glance_only
        self.__swiftMaxSegments = swift_max_segments
        self.__swiftUseSlo = swift_use_slo
        self.__use_dr = use_dr
        self.__ignoreSslCert = ignore_ssl_cert
        self.__db_write_cache_size = db_write_cache_size
        self.__privateContainer = private_container
        self.__availabilityZone = availability_zone
        self.__multiThreadedQueue = multithread_queue
        
        if compression: # in case compression is int 
            self.__compression = True
        else:
            self.__compression = False

        super(OpenStackCloudOptions, self).__init__()

        
        
    def generateUploadChannel(
            self, targetsize, targetname=None, targetid = None, resume = False, imagesize=0, volname=None):
        """
        Generates new upload channel

        Args:
            targetsize: long - target cloud disk size in bytes
            targetname: str - arbitrary description to mark the disk after migration (ignored)
            targetid: str - a cloud-defined path describing the upload 
            resume: Boolean - to recreate disk representation (False) or to reupload (True)
            imagesize: long - image file size in bytes
        """

        if self.__glanceOnly:
            return GlanceUploadChannel.GlanceUploadChannel(
                imagesize, self.__server, self.__tennant, self.__username, self.__password, disk_format=self.__disk_format,
                image_name=targetname, container_format=self.__container_format , version="2" , ignore_ssl_cert = self.__ignoreSslCert)
        
        #TODO: should show timestamp in here
        manifest = UploadManifest.ImageManifestDatabase(
            UploadManifest.ImageDictionaryManifest, self.__manifestPath, None, threading.Lock(),
            increment_depth=self.__increment_depth, db_write_cache_size=self.__db_write_cache_size,
            use_dr=self.__use_dr, resume=resume, volname=volname, target_id=targetid)

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
            disk_name=targetid,
            container_name=self.__swiftContainer,
            compression=self.__compression,
            use_new_channel=self.__useNewChannel,
            ignore_etag=self.__ignoreEtag,
            swift_max_segments=self.__swiftMaxSegments,
            swift_use_slo=self.__swiftUseSlo,
            ignore_ssl_cert = self.__ignoreSslCert,
            private_container = self.__privateContainer,
            manifest=manifest, 
            multithread_queue = self.__multiThreadedQueue)

    def generateInstanceFactory(self):
        return OpenStackInstanceGenerator.OpenStackInstanceGenerator(
            self.__server, self.__tennant, self.__username, self.__password , ip_pool = self.__publicIpPool , ignore_ssl_cert = self.__ignoreSslCert)


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
        return self.__availabilityZone
        
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

