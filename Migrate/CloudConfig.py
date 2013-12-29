# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

#NOTE: not all of these options are implemented for a specific cloud
class CloudConfig(object):
    """Config of the cloud to migrate"""
    
    def __init__(self):
        """
        Creates cloud config
        """
        return

    def generateUploadChannel(self , targetsize , targetname = None, targetid = None, resume_upload = False):
        """
        Generates new upload channel

        Args:
            targetsize - size of arget object to upload in bytes
            targetname - optional name to describe target
            targetid - id of already uploaded target. needed for resume upload operations
            resume_upload - whether to resume previous upload

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