"""
LinuxVolumeInfo
~~~~~~~~~~~~~~~~~

This module provides LinuxVolumeInfo class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import subprocess
import re

from MigrateExceptions import FileException

import struct
import VolumeInfo

class LinuxVolumeInfo(VolumeInfo.VolumeInfo):
    """Description of one FS volume in a system: Windows implementation"""
    
    def __init__(self , path):
        self.__rootPath = path
        super(LinuxVolumeInfo, self).__init__()

    #gets the size of volume in bytes
    def getSize(self):
        filename = self.__rootPath
        try:
            #TODO: should use IOCTL instead of this function
            command = "fdisk -l " + filename  + " | grep ^Disk | awk '{print $5}'"
            size = long(subprocess.check_output(["/bin/sh", command]))
            return size
        except Exception as ex:
            raise FileException(filename , ex)

    def getUsedSize(self):
        return 0

    def getFreeSize(self):
        return 0

    # gets the iterable of system pathes to fs root of mounted volume
    def getMointPoints(self):
        return None

    # gets the system path of volume block device
    def getDevicePath(self):
        return self.__rootPath