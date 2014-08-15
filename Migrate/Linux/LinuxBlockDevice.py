"""
LinuxBlockDevice
~~~~~~~~~~~~~~~~~

This module provides LinuxBlockDevice class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
from DataExtent import DataExtent


from MigrateExceptions import *
import BackupSource

import subprocess
import LinuxVolumeInfo

class DeferedReader(object):
    """Defers reading extent from the volume extent"""
    def __init__(self, volExtent, volume):
        self.__volExtent = volExtent
        self.__volume = volume
    def __str__(self):
        return self.__volume.readExtent(self.__volExtent)

class LinuxBlockDevice(BackupSource.BackupSource):
    """Backup source in Windows OS"""
    
    
    def __init__(self, dev_name , max_reported_extent_size=16*1024*1024):
        """
        constructor

        Args:
        dev_name :str - path to the dev to be opened
        max_reported_extent_size: int, long - size of maximum extent size reported by any of funcitons that gets extents.
        If the extent is larger it is split on few smaller ones of size <= max_reported_extent_size

        """
       
        self.__maxReportedExtentSize = max_reported_extent_size
        
        self.__devName = dev_name

        logging.debug("Openning volume %s" , self.__devName) 
      
        self.__fDev = open(self.__devName , "rb")

        return 

    # gets the volume size
    def getVolumeSize(self):
        return LinuxVolumeInfo.LinuxVolumeInfo(self.__devName).getSize()

    # gets the file enumerator (iterable). each of it's elements represent a filename (unicode string)
    # one may specify another mask
    def getFileEnumerator(self , rootpath = "\\" , mask = "*"):
        return None

    def getVolumeName(self):
        return self.__devName
    
    # returns iterable of DataExtent structs with data filled
    def getFileDataBlocks(self, fileName):
        return None

    #gets filesystem string 
    def getFileSystem(self):
        return None

    #returns iterable of filled bitmap extents with getData available
    def getFilledVolumeBitmap(self):
        devsize = self.getVolumeSize()
        last_start = 0
        last_size = self.__maxReportedExtentSize
        volextents = list()
        while last_start < devsize:
           
           if devsize - last_start > self.__maxReportedExtentSize:
               last_size = self.__maxReportedExtentSize
           else:
               last_size = devsize - last_start
           volextent = DataExtent(last_start, last_size)
           volextent.setData(DeferedReader(volextent, self) )
           volextents.append(volextent)
           last_start = last_start + last_size
        
        return volextents

    #returns bytes read
    def readExtent(self, volextent):
        filename = self.__volumeName
        output = ""
        try:
            # we should read several blocks. Big chunks of data read could cause vss to fail
            size = volextent.getSize()
            self.__fDev.seek(volextent.getStart())

            while size > self.__maxReportedExtentSize:
                (result , partoutput) = self.__fDev.read(self.__maxReportedExtentSize)
                output = output + partoutput # output.extend(partoutput)
                size = size - self.__maxReportedExtentSize
            partoutput = self.__fDev.read(size)
            output = output + partoutput
        except Exception as ex:
            raise FileException(filename , ex)
        return str(output)

    def lock(self):
        return 
    

    