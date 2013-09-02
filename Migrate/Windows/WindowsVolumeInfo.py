# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys
sys.path.append(sys.path[0]+'\\..')

import subprocess
import re

import WindowsVolumeTransferTarget
from MigrateExceptions import FileException

import win32file
import win32event
import win32con
import win32security
import win32api
import pywintypes
import ctypes
import winioctlcon
import struct
import ntsecuritycon
import VolumeInfo

class WindowsVolumeInfo(VolumeInfo.VolumeInfo):
    """Description of one FS volume in a system: Windows implementation"""
    
    def __init__(self , path):
        self.__rootPath = path
        super(WindowsVolumeInfo, self).__init__()

    #gets the size of volume in bytes
    def getSize(self):
        filename = self.__rootPath
        try:
            #TODO: should use IOCTL instead of this function
            (sectors_per_cluster, bytes_per_sector, free_clusters , total_clusters) = win32file.GetDiskFreeSpace(self.__rootPath)
        except Exception as ex:
            raise FileException(filename , ex)
        # it seems like 1 extra cluster is needed in order to store extra $bootfile in a partition
        return long((total_clusters+1)*sectors_per_cluster*bytes_per_sector)

    def getUsedSize(self):
        return self.getSize() - self.getFreeSize();

    def getFreeSize(self):
        filename = self.__rootPath
        try:
            (sectors_per_cluster, bytes_per_sector, free_clusters , total_clusters) = win32file.GetDiskFreeSpace(self.__rootPath)
        except Exception as ex:
            raise FileException(filename , ex)
        return long(free_clusters*sectors_per_cluster*bytes_per_sector)

    # gets the iterable of system pathes to fs root of mounted volume
    def getMointPoints(self):
        filename = self.__rootPath
        try:
            unique_name = win32file.GetVolumeNameForVolumeMountPoint(self.__rootPath)
            return win32file.GetVolumePathNamesForVolumeName(unique_name)
        except Exception as ex:
            raise FileException(filename , ex)

    # gets the system path of volume block device
    def getDevicePath(self):
        return self.__rootPath
        
