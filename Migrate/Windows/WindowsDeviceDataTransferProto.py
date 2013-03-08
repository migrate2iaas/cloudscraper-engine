import DataTransferProto

import struct
import re

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

import logging


from MigrateExceptions import FileException

class WindowsDeviceDataTransferProto(DataTransferProto.DataTransferProto):
    """The class implements data transfer thru Windows API rw calls"""

    def __init__(self,devPath,devnumber):
        self.__devicePath = devPath
        self.__devNumber = devnumber
        
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()

        path = self.__devicePath
        try:
            logging.debug("Openning Win32 device transfer protocol , device = " + self.__devicePath);
            self.__hFile = win32file.CreateFile( path, win32con.GENERIC_READ | win32con.GENERIC_WRITE| ntsecuritycon.FILE_READ_ATTRIBUTES | ntsecuritycon.FILE_WRITE_ATTRIBUTES, win32con. FILE_SHARE_READ|win32con.FILE_SHARE_WRITE, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        except Exception as ex:
            raise FileException(path , ex)
      
    def writeData(self, dataExtent):
        return writeMetadata(self,dataExtent)
        

    def readData(self, dataExtent):
        return readMetadata(self,dataExtent)

    def writeMetadata(self, dataExtents):
        path = self.__devicePath
        try:
            for volextent in dataExtent:
                win32file.SetFilePointer(self.__hFile, volextent.getStart(), win32con.FILE_BEGIN)
                win32file.WriteFile(self.__hFile,volextent.getData(),None)
        except Exception as ex:
            raise FileException(path , ex)
        return

    def readMetadata(self, dataExtent):
        path = self.__devicePath
        try:
            win32file.SetFilePointer(self.__hFile, dataExtent.getStart(), win32con.FILE_BEGIN)
            (result , output) = win32file.ReadFile(self.__hFile,dataExtent.getSize(),None)
        except Exception as ex:
            raise FileException(path , ex)
        return output

    #returns the overall size of underlying media
    def getSize(self):
         # typedef struct {
            #LARGE_INTEGER Length;
            #} GET_LENGTH_INFORMATION;
        path = self.__devicePath
        try:
            outbuffersize = 8
            IOCTL_DISK_GET_LENGTH_INFO = 0x7405c
            outbuffer = win32file.DeviceIoControl(self.__hFile, IOCTL_DISK_GET_LENGTH_INFO , None , outbuffersize , None )
        except Exception as ex:
            raise FileException(path , ex)
        return struct.unpack("@q" , outbuffer)[0]

    # Windows-only function to get it's device number
    def getDeviceNumber(self):
         return self.__devNumber

    # Windows-only function to send ioctls to Windows device
    def sendControlCode(self , ioctl , inBuffer, outBufferSizeMax):
        path = self.__devicePath
        try:
            outbuffer = win32file.DeviceIoControl(self.__hFile, ioctl , inBuffer , outBufferSizeMax , None )
        except Exception as ex:
            raise FileException(path , ex)
        return outbuffer

    