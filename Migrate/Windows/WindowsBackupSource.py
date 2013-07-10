# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys
import os

import win32file
import win32event
import win32con
import win32security
import win32api
import pywintypes

from MigrateExceptions import *
import BackupSource
import WindowsFileToBackup

class WindowsBackupSource(BackupSource.BackupSource):
    """Backup source in Windows OS"""

    #
    #__volumeDataSource = null

    def __init__ (self):
        self.__volumeDataSource = None
        return

    # gets enumerator of FilesToBackup type
    def getFileEnum(self):
        if self.__volumeDataSource == None:
            raise PropertyNotInitialized("self.__volumeDataSource", " Use setBackupDataSource() to init it")
        return WindowsFileToBackup.WindowsFileIterator(self.__volumeDataSource.getFileEnumerator() , self)

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