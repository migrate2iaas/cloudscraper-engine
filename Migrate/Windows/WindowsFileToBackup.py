# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import FileToBackup
from DataExtent import DataExtent
import WindowsVolume

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

from MigrateExceptions import FileException

#NOTE: use context manager for cleanups, 'cause there is no good destructors in python 
#from contextlib import contextmanager

class WindowsFileToBackup(FileToBackup.FileToBackup):
    """FileToBackup standard implementation for Windows system"""

    def __init__(self, srcPath , backupSource):

        self.__name = srcPath
        self.__srcPath = srcPath
        self.__backupSource = backupSource
        
        self.__hFile = None

        #TODO: set dest path
        self.__destPath = None
        self.__transferDest = None
        return


    def getName(self):
        return self.__name.lower()

    def getDestPath(self):
        return self.__destPath.lower()

    def getSourcePath(self):
        return self.__srcPath.lower()

    def getBackupSource(self):
        return self.__backupSource

    def getTransferDest(self):
        return self.__transferDest

    def setDestPath(self , path):
        self.__destPath = path

    def setTransferDest(self , dest):
        self.__transferDest = dest


    def getChangedExtents(self):
        self.__reopen()
        filename = self.__name
        try:
            size = win32file.GetFileSize(self.__hFile)
        except Exception as ex:
            raise FileException(filename , ex)
        return DataExtent(0 , size)

    #returns data read
    def readData(self,volextent):
        self.__reopen()
        filename = self.__name
        try:
            win32file.SetFilePointer(self.__hFile, volextent.getStart(), win32con.FILE_BEGIN)
            (result , output) = win32file.ReadFile(self.__hFile,volextent.getSize(),None)
        except Exception as ex:
            raise FileException(filename , ex)
        

        self.__close()
        return output

    #reopens source file if needed
    def __reopen(self):
        if self.__hFile == None:
            filename = self.getSourcePath()
            try:
                secur_att = win32security.SECURITY_ATTRIBUTES()
                secur_att.Initialize()
                self.__hFile = win32file.CreateFile( filename, win32con.GENERIC_READ | win32con.READ_CONTROL | ntsecuritycon.FILE_READ_ATTRIBUTES | ntsecuritycon.FILE_WRITE_ATTRIBUTES, win32con. FILE_SHARE_READ|win32con.FILE_SHARE_WRITE, secur_att,   win32con.OPEN_ALWAYS, win32con.FILE_ATTRIBUTE_NORMAL|win32con. FILE_FLAG_BACKUP_SEMANTICS , 0 )
            except Exception as ex:
                raise FileException(filename , ex)
            
    
    def __close(self):
        if self.__hFile != None:
            filename = self.__name
            try:
                win32file.CloseHandle(self.__hFile)
            except Exception as ex:
                raise FileException(filename , ex)
            self.__hFile = None


class WindowsFileIterator(object):
    """Iterator thru windows files"""

    
    def __init__(self, filepathIterator, backupSource):
        self.__filePathIterator = filepathIterator
        self.__backupSource = backupSource
    
    def __iter__(self):
        return self

    #returns name of the file
    def next(self):
        filepath = self.__filePathIterator.next()
        winfile = WindowsFileToBackup(filepath ,  self.__backupSource)
        return winfile
        
