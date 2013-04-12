# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

#TODO; inherit from the common system info class
class SystemInfo(object):
    """Base class for system Info"""

    Archi386 = 0x0386
    Archx8664 = 0x8664
    ArchI64 = 0x0064
    ArchUnknown = 0x0000

    def __init__(self):
        return

    # gets arbitary-way formatted string describing the current system
    def getSystemVersionString(self):
        raise NotImplementedError

    def getKernelVersion(self):
        raise NotImplementedError
        
    def getSystemArcheticture(self):
        raise NotImplementedError
        
    #gets system volume info, one where kernel/drivers is situated
    def getSystemVolumeInfo(self):
        raise NotImplementedError
        
    # gets iterable to iterate thru volumes in system
    def getDataVolumesInfo(self):
        raise NotImplementedError
        
    def getHardwareInfo(self):
        raise NotImplementedError
        
    def getNetworkInfo(self):
        raise NotImplementedError
        