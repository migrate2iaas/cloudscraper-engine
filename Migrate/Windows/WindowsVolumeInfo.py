import sys
sys.path.append(sys.path[0]+'\\..')

import subprocess
import re

import WindowsVolumeTransferTarget


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
        return super(WindowsVolumeInfo, self).__init__()

    #gets the size of volume in bytes
    def getSize(self):

        (sectors_per_cluster, bytes_per_sector, free_clusters , total_clusters) = win32file.GetDiskFreeSpace(self.__rootPath)
        return long(total_clusters*sectors_per_cluster*bytes_per_sector)

    def getUsedSize(self):
        return self.getSize() - self.getFreeSize();

    def getFreeSize(self):
        (sectors_per_cluster, bytes_per_sector, free_clusters , total_clusters) = win32file.GetDiskFreeSpace(self.__rootPath)
        return long(free_clusters*sectors_per_cluster*bytes_per_sector)

    # gets the iterable of system pathes to fs root of mounted volume
    def getMointPoints(self):
        unique_name = win32file.GetVolumeNameForVolumeMountPoint(self.__rootPath)
        return win32file.GetVolumePathNamesForVolumeName(unique_name)

    # gets the system path of volume block device
    def getDevicePath(self):
        return self.__rootPath
        
