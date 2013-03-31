
#NOTE: not all of these options are implemented for a specific cloud
class CloudConfig(object):
    """Config of the cloud to migrate"""
    
    def __init__(self):
        return
        
    def getCloudStorage(self):
        raise NotImplementedError

    def getCloudUser(self):
        raise NotImplementedError
    
    def getCloudPass(self):
        raise NotImplementedError
    
    def getNewSystemSize(self):
        raise NotImplementedError

    def getTargetCloud(self):
        raise NotImplementedError

    def getArch(self):
        raise NotImplementedError

    def getZone(self):
        raise NotImplementedError

    def getRegion(self):
        raise NotImplementedError

    def getSecurity():
        raise NotImplementedError

