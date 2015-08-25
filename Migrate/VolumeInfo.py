# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

class VolumeInfo(object):
    """Description of one FS volume in a system"""
    
    def __init__(self):
        return

    #gets the size of volume in bytes
    def getSize(self):
        raise NotImplementedError

    def getUsedSize(self):
        raise NotImplementedError

    def getFreeSize(self):
        raise NotImplementedError

    # gets the iterable of system pathes to fs root of mounted volume
    def getMointPoints(self):
        raise NotImplementedError

    # gets the system path of volume block device
    def getDevicePath(self):
        raise NotImplementedError


