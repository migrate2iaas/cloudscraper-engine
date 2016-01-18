# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import os

class CloudConfig(object):
    """Config of the cloud to migrate"""
    
    def __init__(self):
        """
        Creates cloud config
        """
        return

    def generateUploadChannel(self , targetsize , targetname = None, targetid = None, resume_upload = False , imagesize = 0 , preserve_existing_data = False):
        """
        Generates new upload channel

        Args:
            targetsize - size of resulting object (disk) in the target cloud in bytes
            targetname - optional name to describe target disk
            targetid - cloud id of already uploaded target. 
            resume_upload - whether to resume previous upload
            imagesize - size of the image file (not resulting disk size) to upload. Could be 0. 
            preserve_existing_data: bool - if preserve (make versioned copy) of existing data (only if resume is true)
        """
        raise NotImplementedError
        
    def generateInstanceFactory(self):
        """returns object of InstanceFactory type to create servers from uploaded images"""
        raise NotImplementedError

    def getCloudStorage(self):
        """gets cloud storage where upload is to be performed e.g. S3 bucket"""
        raise NotImplementedError

    def getCloudUser(self):
        """user account to access storage"""
        raise NotImplementedError
    
    def getCloudPass(self):
        """gets password to access storage"""
        raise NotImplementedError
    
    def getNewSystemSize(self):
        """gets new size of system volume in bytes"""
        raise NotImplementedError

    def getTargetCloud(self):
        """gets target cloud name"""
        raise NotImplementedError

    def getArch(self):
        """gets target architicture: i386 or x86_64"""
        raise NotImplementedError

    def getZone(self):
        """gets zone in a region where to launch server if available"""
        raise NotImplementedError

    def getRegion(self):
        """gets cloud region"""
        raise NotImplementedError

    def getSecurity(self):
        """gets id of security group (or firewall settings name) to be launched at"""
        raise NotImplementedError

    def getUploadChunkSize(self):
        """gets upload chunk size"""
        raise NotImplementedError

    def getInstanceType(self):
        """gets intance type: what resources to allocate for a new cloud server. The format of the type is cloud-dependent"""
        raise NotImplementedError

    def getServerName(self):
        """gets new server name"""
        raise NotImplementedError

    def getSubnet(self):
        """gets subnet\vpc identifier"""
        raise NotImplementedError

    def getTargetOS(self):
        """by default it's the same as ours"""
        if os.name == 'nt':
            return 'Windows'
        else:
            return 'Linux'