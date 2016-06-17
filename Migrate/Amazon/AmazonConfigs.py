# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import CloudConfig
import MigrateConfig
import S3UploadChannel
import EC2InstanceGenerator
import EC2MinipadInstanceGenerator
import UploadManifest
import threading


class AmazonCloudOptions(CloudConfig.CloudConfig):
    
    def __init__(
            self, bucket, user, password, newsize, arch, zone, region, machinename, securityid='',
            instancetype='m1.small', chunksize=10*1024*1024, disktype='VHD', keyname_prefix='', vpc="",
            custom_host="", custom_port=80, custom_suffix="", use_ssl=True, minipad = False , minipad_ami="",
            manifest_path="", increment_depth=1, use_dr=False, os_override=None, db_write_cache_size=20):

        """inits with options"""
        super(AmazonCloudOptions, self).__init__()
        self.__bucket = bucket
        self.__user = user
        self.__pass = password
        self.__newSysSize = newsize #deprecated
        self.__arch = arch
        self.__zone = zone
        self.__region = region
        self.__securityGroup = securityid
        self.__chunkSize = chunksize
        self.__instanceType = instancetype
        self.__machineName = machinename
        self.__diskType = disktype
        #small fix to recognize more raw types
        if "RAW" in str(disktype).upper():
            self.__diskType = "RAW"
        self.__keynamePrefix = keyname_prefix
        self.__vpc = vpc
        self.__custom_host = custom_host 
        self.__custom_port = custom_port
        self.__custom_suffix = custom_suffix
        self.__use_ssl = bool(use_ssl)
        self.__minipad = minipad
        self.__minipadAmi = minipad_ami
        self.__manifest_path = manifest_path
        self.__increment_depth = increment_depth
        self.__use_dr = use_dr
        self.__os = os_override
        self.__db_write_cache_size = db_write_cache_size

        #TODO: more amazon-specfiic configs needed
    
    def generateUploadChannel(self, targetsize, targetname=None, targetid=None, resume=False, imagesize=0, volname='system'):
        """
        Generates new upload channel

        Args:
            targetsize: long - target cloud disk size in bytes
            targetname: str - arbitrary description to mark the disk after migration (ignored)
            targetid: str - a cloud-defined path describing the machine name
            resume: Boolean - to recreate disk representation or to reupload
            imagesize: long - image file size in bytes
            volname - volume letter or descrption, used to generate cloud path
        """
        # check if we use custom (non AWS) S3 
        custom = False
        if self.__custom_host:
            custom = True

        manifest = UploadManifest.ImageManifestDatabase(
            UploadManifest.ImageDictionaryManifest, self.__manifest_path, None, threading.Lock(),
            increment_depth=self.__increment_depth, db_write_cache_size=self.__db_write_cache_size,
            use_dr=self.__use_dr, resume=resume, volname=volname, target_id=targetid)

        return S3UploadChannel.S3UploadChannel(
            self.__bucket, self.__user, self.__pass, targetsize, self.__custom_host or self.__region,
            self.__diskType, chunksize=self.__chunkSize, walrus=custom, walrus_path=self.__custom_suffix,
            walrus_port=self.__custom_port, use_ssl=self.__use_ssl, make_link_public=True, manifest=manifest)
         
    def generateInstanceFactory(self):
        """returns object of InstanceFactory type to create servers from uploaded images"""
        #No migratiuons to custom host as for now
        if self.__custom_host:
            return None
        if self.__minipad:
            return EC2MinipadInstanceGenerator.EC2MinipadInstanceGenerator(
                self.__region, self.__minipadAmi, self.__user, self.__pass, self.__zone, self.__instanceType,
                self.__vpc, self.__securityGroup)
        return EC2InstanceGenerator.EC2InstanceGenerator(self.__region)

    def getCloudStorage(self):
        return self.__bucket

    def getCloudUser(self):
        return self.__user
    
    def getCloudPass(self):
        return self.__pass

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
        return self.__machineName

    def getSubnet(self):
        return self.__vpc

    def getTargetOS(self):
        """by default it's the same as ours"""
        if self.__os:
            return self.__os
        return super(AmazonCloudOptions, self).getTargetOS()

class AmazonMigrateConfig(MigrateConfig.MigrateConfig):

    #TODO: make docs
    # images is list of VolumeMigrateConfig
    def __init__(self, images , media_factory , source_arch , imagetype , insert_xen = False):
        super(AmazonMigrateConfig, self).__init__(images, media_factory)

        self.__imageArch = source_arch
        #do we need few images? dunno...
        self.__imageType = imagetype
        #small fix to recognize more raw types
        if "RAW" in str(imagetype).upper():
            self.__imageType = "RAW"
        self.__minipadXen = insert_xen

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

    def insertVirtIo(self):
        return False

    def insertXen(self):
        return self.__minipadXen

