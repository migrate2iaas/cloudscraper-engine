import sys
import os

import win32file
import win32event
import win32con
import win32security
import win32api
import pywintypes


class WindowsBackupSource(BackupSource):
    """Backup source in Windows OS"""


    __volumeDataSource = null

    # gets files enumerator
    def getFileEnum():
        __volumeDataSource
        return

    # gets block range for range of files
    # backupOptions - options incl file excludes, etc
    def getFilesBlockRange(backupOptions):
        
        return

    # sets the data source, should be Win32 Volume
    def setBackupDataSource(dataSource):
        __volumeDataSource = dataSource
        return

    # gets the data source
    def getBackupDataSource():
        return __volumeDataSource

    #TODO: adds metadata blocks also