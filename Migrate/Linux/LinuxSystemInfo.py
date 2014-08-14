"""
LinuxSystemInfo
~~~~~~~~~~~~~~~~~

This module provides LinuxSystemInfo class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback


import LinuxVolumeInfo
import SystemInfo

#TODO; inherit from the common system info class
class LinuxSystemInfo(SystemInfo.SystemInfo):
    """System Info of Windows system"""

   

    def __init__(self):

        super(LinuxSystemInfo, self).__init__()

        #NOTE: here we should get distro info
    
        return

    # gets arbitary-way formatted string describing the current system
    def getSystemVersionString(self):
        #TODO: get distro
        return "Linux"


    def getKernelVersion(self):
        #TODO: get kernel
        return "kernel"

    def getSystemArcheticture(self):
        return self.Archx8664

    #gets system volume info, one where kernel/drivers is situated
    def getSystemVolumeInfo(self):
        return LinuxVolumeInfo.LinuxVolumeInfo("/dev/sda")

    #gets the volume info by its path
    def getVolumeInfo(self , path):
        return LinuxVolumeInfo.LinuxVolumeInfo(path)

    # gets iterable to iterate thru volumes in system
    def getDataVolumesInfo(self):
        #TODO: implement iterator
        return

    def getHardwareInfo(self):
        #TODO: implement
        return

    def getNetworkInfo(self):
        #TODO: implement
        return