import TransferTarget
import FileToBackup
import DataExtent
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

class WindowsVolumeTransferTarget(TransferTarget.TransferTarget):
    """Transfer target represented as Windows (NTFS) volume"""

    def __init__(self, volumeName):
        
        
        
        self.__volumeName = volumeName.lower()
        #self.__hfile = win32file.CreateFile( self.__volumeName, win32con.GENERIC_READ | win32con.GENERIC_WRITE | ntsecuritycon.FILE_READ_ATTRIBUTES | ntsecuritycon.FILE_WRITE_ATTRIBUTES, win32con. FILE_SHARE_READ|win32con.FILE_SHARE_WRITE, secur_att,   win32con.OPEN_ALWAYS, win32con.FILE_ATTRIBUTE_NORMAL , 0 )

    # writes the file
    def TransferFile(self, fileToBackup):
         raise NotImplementedError

    # transfers file data only, no metadata should be written
    def TransferFileData(self, fileName, fileExtentsData):
         raise NotImplementedError

    # transfers file data only
    def TransferRawData(self, volumeDataExtents):
        # open as raw
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        hfile = win32file.CreateFile( self.__volumeName, win32con.GENERIC_READ | win32con.GENERIC_WRITE | ntsecuritycon.FILE_READ_ATTRIBUTES | ntsecuritycon.FILE_WRITE_ATTRIBUTES, win32con. FILE_SHARE_READ|win32con.FILE_SHARE_WRITE, secur_att,   win32con.OPEN_ALWAYS, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        # lock it
        outbuffer = win32file.DeviceIoControl(hfile,  winioctlcon.FSCTL_LOCK_VOLUME,  None, None, None )
        # writing data
        # note: maybe to use multithreading in here, dunno
        for volextent in volumeDataExtents:
            win32file.SetFilePointer(hfile, volextent.getStart(), win32con.FILE_BEGIN)
            win32file.WriteFile(hfile,volextent.getData(),None)
          
        win32file.CloseHandle(hfile)

    # transfers raw metadata, it should be precached
    def TransferRawMetadata(self, volumeDataExtents):
        # the same for now
        return self.TransferRawData(volumeDataExtents)

