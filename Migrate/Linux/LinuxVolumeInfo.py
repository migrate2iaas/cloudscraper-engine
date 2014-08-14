"""
LinuxVolumeInfo
~~~~~~~~~~~~~~~~~

This module provides LinuxVolumeInfo class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

from subprocess import *
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

            p1 = Popen(["fdisk" , "-l" ,filename], stdout=PIPE)
            p2 = Popen(["grep", "^Disk"], stdin=p1.stdout, stdout=PIPE)
            p3 = Popen(["awk", "'{print $5}'"], stdin=p2.stdout, stdout=PIPE)
            output = p2.communicate()[0]

            logging.info ("Got " + filename + " size = " + output)
            
            size = long(output)
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