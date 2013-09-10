# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys
import os

sys.path.append('.\Windows')

import WindowsVolume
import WindowsBackupSource
import AdjustedBackupSource
import BackupAdjust
import WindowsVolumeTransferTarget
import VssThruVshadow

import WindowsVolume
import WindowsBackupSource
import AdjustedBackupSource
import BackupAdjust
import WindowsSystemAdjustOptions
import WindowsVhdMedia
import WindowsDiskParser
import WindowsDeviceDataTransferProto
import WindowsSystemInfo

import unittest
import shutil
import logging

class Windows(object):
    """factory class for generating objects of Windows subsystem"""

    def __init__(self):
        self.__filesToDelete = set()
        self.__vss = VssThruVshadow.VssThruVshadow()
        return
    
    # volume should be in "\\.\X:" form
    def getDataBackupSource(self , volume):
        
        snapname = self.__vss.createSnapshot(volume)
      
        WinVol = WindowsVolume.WindowsVolume(snapname)

        self.rollbackSystemVolumeChanges()
        return WinVol
        

    def getSystemDataBackupSource(self):
        logging.debug("Getting the system backup source") 
        windir = os.environ['Windir'] 
        windir = windir.lower()
        windrive_letter = windir[0] 
        system_vol = "\\\\.\\"+windrive_letter+":"

        self.makeSystemVolumeBootable()

        snapname = self.__vss.createSnapshot(system_vol)
      
        WinVol = WindowsVolume.WindowsVolume(snapname)

        self.rollbackSystemVolumeChanges()
        return WinVol

    #frees the snapshot
    def freeDataBackupSource(self , vol):
        
        volumename = vol.getVolumeName()
        self.__vss.deleteSnapshot(volumename)

    def makeSystemVolumeBootable(self):
        originalwindir = os.environ['windir']
        windrive = originalwindir.split("\\")[0] #get C: substring
        bootdir = windrive+"\\Boot"
        
        
        #TODO: set right permissions to it
        if (os.path.exists(windrive+"\\bootmgr")) == False:
            logging.debug("Bootmgr not found in Windows root , copying it there")       
            shutil.copy2(originalwindir + "\\Boot\\PCAT\\bootmgr", windrive+"\\bootmgr")
            self.__filesToDelete.add(windrive+"\\bootmgr")

        if (os.path.exists(bootdir) and os.path.exists(bootdir+"\\BCD")):
            return
        else:
            if os.path.exists(bootdir) == False:
                #create a new one here
                try:
                    logging.debug("Creating a bootdir")       
                    shutil.copytree(originalwindir + "\\Boot\\PCAT" , bootdir)
                    self.__filesToDelete.add(bootdir)
                except:
                    logging.error("Cannot create bootdir")       
                    #TODO log error
                    return

            logging.debug("Creating an empty BCD store")       
            # we create an empty BCD so it'll be altered after the transition
            bcdfile = open(bootdir+"\\BCD", "w")
            nullbytes = bytearray(64*1024)
            bcdfile.write(nullbytes)


    def rollbackSystemVolumeChanges(self):
        for file in  self.__filesToDelete:
            logging.debug("Deleting temporary file" + file)       
            shutil.rmtree(file , True , None)
            #TODO: log failures


    def createVhdMedia(self , path , imagesize):
        #TODO: maybe it's good idea to use WindowsVhdMediaFactory here
        vhd = WindowsVhdMedia.WindowsVhdMedia(path, imagesize)
        return vhd

    def createVhdTransferTarget(self , path , size , adjustOptions):
        logging.debug("Creating VHD transfer target")
        gigabyte = 1024*1024*1024
        vhd = self.createVhdMedia(path, size+gigabyte)
        vhd.open()
          
        #really this device data transfer proto is not used, it seems to be a connector between raw media and methods to access its contents
        datatransfer = WindowsDeviceDataTransferProto.WindowsDeviceDataTransferProto(vhd.getWindowsDevicePath(), vhd.getWindowsDiskNumber() , vhd)
        parser = WindowsDiskParser.WindowsDiskParser(datatransfer , adjustOptions.getNewMbrId())
        return parser.createTransferTarget(size)
        
    def createSystemAdjustOptions(self):
        return WindowsSystemAdjustOptions.WindowsSystemAdjustOptions(self.getVersion() < WindowsSystemInfo.WindowsSystemInfo.Win2008R2)

    def getVersion(self):
        return WindowsSystemInfo.WindowsSystemInfo().getKernelVersion()

    def getSystemInfo(self):
        return WindowsSystemInfo.WindowsSystemInfo()


# Some helpers here

import sys

def win32_unicode_argv():
    """Uses shell32.GetCommandLineArgvW to get sys.argv as a list of Unicode
    strings.

    Versions 2.x of Python don't support Unicode in sys.argv on
    Windows, with the underlying Windows API instead replacing multi-byte
    characters with '?'.
    """

    from ctypes import POINTER, byref, cdll, c_int, windll
    from ctypes.wintypes import LPCWSTR, LPWSTR

    GetCommandLineW = cdll.kernel32.GetCommandLineW
    GetCommandLineW.argtypes = []
    GetCommandLineW.restype = LPCWSTR

    CommandLineToArgvW = windll.shell32.CommandLineToArgvW
    CommandLineToArgvW.argtypes = [LPCWSTR, POINTER(c_int)]
    CommandLineToArgvW.restype = POINTER(LPWSTR)

    cmd = GetCommandLineW()
    argc = c_int(0)
    argv = CommandLineToArgvW(cmd, byref(argc))
    if argc.value > 0:
        # Remove Python executable and commands if present
        start = argc.value - len(sys.argv)
        return [argv[i] for i in
                xrange(start, argc.value)]

    