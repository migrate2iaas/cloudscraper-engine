"""
LinuxBackupSource
~~~~~~~~~~~~~~~~~

This module provides LinuxBackupSource class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback

import gcimagebundlelib
import FileToBackup

   
from MigrateExceptions import *
import BackupSource



class LinuxFileToBackup(FileToBackup.FileToBackup):
    """FileToBackup standard implementation for Windows system"""

    def __init__(self, srcPath , backupSource):

        self.__name = srcPath
        self.__srcPath = srcPath
        self.__backupSource = backupSource
        

        #TODO: set dest path
        self.__destPath = None
        self.__transferDest = None
        return

    def __str__(self):
        return self.getName()

    def getName(self):
        return self.__name

    def getDestPath(self):
        return self.__destPath

    def getSourcePath(self):
        return self.__srcPath

    def getBackupSource(self):
        return self.__backupSource

    def getTransferDest(self):
        return self.__transferDest

    def setDestPath(self , path):
        self.__destPath = path

    def setTransferDest(self , dest):
        self.__transferDest = dest


    def getChangedExtents(self):
        raise NotImplementedError

    #returns data read
    def readData(self,volextent):
        raise NotImplementedError

    #reopens source file if needed
    def __reopen(self):
        raise NotImplementedError
            
    
    def __close(self):
        raise NotImplementedError


class LinuxBackupSource(BackupSource.BackupSource):
    """Backup source in Linux OS"""

    #
    #__volumeDataSource = null

    def __init__ (self , root = "/"):
        self.__volumeDataSource = None
        return

    # gets enumerator of FilesToBackup type
    def getFileEnum(self, root = "/" , mask = "*"):
        if self.__volumeDataSource == None:
            raise PropertyNotInitialized("self.__volumeDataSource", " Use setBackupDataSource() to init it")
        if self.__volumeDataSource:
            if self.__volumeDataSource.getVolumeName():
                enum = self.__volumeDataSource.getFileEnumerator(root , mask)
                if enum:
                    return enum
         #TODO: Just workaround, should rebuild
        return [LinuxFileToBackup(str(root), self)].__iter__()

    # gets block range list for all the files. Note: it should be ordered
    def getFilesBlockRange(self):
        if self.__volumeDataSource == None:
            raise PropertyNotInitialized("self.__volumeDataSource", " Use setBackupDataSource() to init it")
        # in case we use all the volume and then make the excludes
        return self.__volumeDataSource.getFilledVolumeBitmap()

    # sets the data source, should be Win32 Volume
    def setBackupDataSource(self, dataSource):
        self.__volumeDataSource = dataSource
        return

    # gets the data source
    def getBackupDataSource(self):
        return self.__volumeDataSource

    #returns iterable of block ranges, sorted from the file logical begining to the end
    def getFileBlockRange(self,filename):
        #TODO: return empty list on error
        return self.__volumeDataSource.getFileDataBlocks(filename)

    #TODO: adds metadata blocks also