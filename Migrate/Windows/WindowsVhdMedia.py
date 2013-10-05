# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import subprocess
import re
import os
import sys

sys.path.append('.\..')
sys.path.append('.\..\Windows')
sys.path.append('.\Windows')


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
import os
import logging
import ImageMedia

from ctypes import *

from MigrateExceptions import *

# TODO: make media base class
class WindowsVhdMedia(ImageMedia.ImageMedia):
    """VHD disk created and managed by Win2008R2+ systems"""

    #it should generate RW-access (protocol to write data) so it could be accessed from elsewhere


    # 
    def __init__(self, filename, maxInBytes):
        # TODO: it's better to use CreateVirtualDisk() from WinAPI to acomplish the task
        # this one is created with diskpart
        logging.info("Initing new VHD disk " + filename + " of size " + str(maxInBytes) + " bytes") 
        self.__fileName = filename
        sizemb = int(maxInBytes/1024/1024)
        if sizemb % (1024*1024):
            sizemb = sizemb + 1
        
        self.__diskNo = diskno = None
        self.__maxSizeMb = sizemb
        self.__hDrive = None

        # Load DLL into memory.
        folder = os.path.dirname(os.path.abspath(__file__))
        dll_path = os.path.join(folder, "..\\vhdwrap.dll") 
        self.__vdiskDll = ctypes.CDLL(dll_path)
        super(WindowsVhdMedia,self).__init__() 

        return 

    #internal function to open drive
    def __openDrive(self):
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        drivename = self.getWindowsDevicePath()
        logging.debug("Openning disk %s" , drivename) 
        filename = drivename

        # data for ioctl to bring the disk online
        # input
        #  typedef struct _SET_DISK_ATTRIBUTES {
        #  DWORD     Version 
        #  BOOLEAN   Persist;
        #  BOOLEAN   Reserved1[3];
        #  DWORDLONG Attributes;
        #  DWORDLONG AttributesMask;
        #  DWORD     Reserved2[4];
        #} SET_DISK_ATTRIBUTES, *PSET_DISK_ATTRIBUTES;
        
        versionsize = struct.calcsize('=IIqqIIII')
        attributes = struct.pack('=IIqqIIII',versionsize,1,0,1,0,0,0,0)
        IOCTL_DISK_SET_DISK_ATTRIBUTES = 0x7c0f4
        try:
            self.__hDrive = win32file.CreateFile( drivename, win32con.GENERIC_READ | win32con.GENERIC_WRITE| ntsecuritycon.FILE_READ_ATTRIBUTES , win32con. FILE_SHARE_READ | win32con. FILE_SHARE_WRITE, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
            outbuffer = win32file.DeviceIoControl(self.__hDrive,  IOCTL_DISK_SET_DISK_ATTRIBUTES ,  attributes, None, None )
        except Exception as ex:
            raise FileException(filename , ex)

    def __closeDrive(self):
        win32file.CloseHandle(self.__hDrive)


    def __createDisk(self):
        self.__vdiskDll.CreateExpandingVhd.restype = c_void_p
        self.__hVirtDisk = self.__vdiskDll.CreateExpandingVhd(c_wchar_p(unicode(self.__fileName)) , c_ulonglong(self.__maxSizeMb*1024*1024))
        if (self.__hVirtDisk == 0 or self.__hVirtDisk == -1):
           logging.error("!!!ERROR: Failed to create virtual disk to store data, error = 0x" + hex(self.__vdiskDll.GetLastVhdError(None)))
           raise WindowsError
        return

    def __openDisk(self):
        self.__vdiskDll.CreateExpandingVhd.restype = c_void_p
        #TODO: check its size
        self.__hVirtDisk = self.__vdiskDll.OpenVhd(c_wchar_p(unicode(self.__fileName)))
        if (self.__hVirtDisk == 0 or self.__hVirtDisk == -1):
           logging.error("!!!ERROR: Failed to create virtual disk to store data, error = 0x" + hex(self.__vdiskDll.GetLastVhdError(None)))
           raise WindowsError
        return

    #returns the disk number
    def __attachDisk(self):
        self.__vdiskDll.CreateExpandingVhd.restype = c_void_p
        success = self.__vdiskDll.AttachVhd(c_void_p(self.__hVirtDisk))
        if success == 0:
           logging.error("!!!ERROR: Failed to attach virtual disk, error = 0x" + hex(self.__vdiskDll.GetLastVhdError(None)))
           raise WindowsError

        diskno = self.__vdiskDll.GetAttachedVhdDiskNumber(c_void_p(self.__hVirtDisk))
        if diskno == -1:
           last_error = self.__vdiskDll.GetLastVhdError(None)
           logging.error("!!!ERROR: Failed to get attached virtual disk path, error = 0x" + hex(last_error))
           raise WindowsError(last_error)

        return diskno

    def __detachDisk(self):
        success = self.__vdiskDll.DetachVhd(c_void_p(self.__hVirtDisk))
        if success == 0:
           logging.error("!!!ERROR: Failed to detach virtual disk, error = 0x" + hex(self.__vdiskDll.GetLastVhdError(None)))
           raise WindowsError
        
    def __closeDisk(self):
        success = self.__vdiskDll.CloseVhd(c_void_p(self.__hVirtDisk))
        if success == 0:
           logging.error("!!!ERROR: Failed to close virtual disk, error = 0x" + hex(self.__vdiskDll.GetLastVhdError(None)))
           raise WindowsError
    
    def open(self):
        if os.path.exists(self.__fileName):
            logging.info(">>>>> Reopening precreated container " + self.__fileName + " . Note: it contents couldn't be changed.")
            self.__openDisk()
        else:
            logging.debug("Initing new VHD disk") 
            self.__createDisk()

        self.__diskNo = self.__attachDisk()
        self.__openDrive()

        return True

    def getMaxSize(self):
        return self.__maxSizeMb*1024*1024

    def reopen(self):
        self.close()
        self.open()
        return

    def close(self):
        self.__closeDrive()
        self.__detachDisk()
        self.__closeDisk()
        
        return True

    def flush(self):
        return
    
    def release(self):
        self.close()


     #reads data from image, returns data buffer
    def readImageData(self , offset , size):
        diskfile = open(self.__fileName, "rb")
        diskfile.seek(offset)
        data = diskfile.read(size)
        diskfile.close()
        return data

    #writes data to the container (as it was a disk)
    def writeDiskData(self, offset, data):
        if self.__hDrive == None:
            raise IOError("Use open first to create vhd disk media")
        filename = self.getWindowsDevicePath()
        try:
            win32file.SetFilePointer(self.__hDrive, offset, win32con.FILE_BEGIN)
            (result , output) = win32file.WriteFile(self.__hDrive,data)
        except Exception as ex:
            logging.error("!!!ERROR: Failed to write to existing disk image. Please specify another image path and restart the operation.")
            raise FileException(filename , ex)
        return output

    #reads data from the container (as it was a disk)
    def readDiskData(self , offset , size):
        if self.__hDrive == None:
            raise IOError("Use open first to create vhd disk media")
        filename = self.getWindowsDevicePath()
        try:
            win32file.SetFilePointer(self.__hDrive, offset, win32con.FILE_BEGIN)
            (result , output) = win32file.ReadFile(self.__hDrive,size,None)
        except Exception as ex:
            raise FileException(filename , ex)
        return output
       
        

    #gets the overall image size available for writing. Note: it is subject to grow when new data is written
    def getImageSize(self):
        return os.stat(self.__fileName).st_size

    #override for WindowsMedia
    # returns path good for opening windows devices
    def getWindowsDevicePath(self):
        return "\\\\.\\PhysicalDrive" + str(self.__diskNo)

    def getWindowsDiskNumber(self):
        return self.__diskNo

    #sets the channel so the data may be sent whenever data changes
    def setChannel(self):
        return