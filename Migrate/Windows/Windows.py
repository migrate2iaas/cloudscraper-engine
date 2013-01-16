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

import unittest
import shutil
import logging

class Windows(object):
    """factory class for generating objects of Windows subsystem"""

    Win2003 = 0x52
    Win2008 = 0x60
    Win2008R2 = 0x61
    Win2012 = 0x62

    def __init__(self):
        self.__filesToDelete = set()
        return

    def getSystemDataBackupSource(self):
        logging.debug("");
        windir = os.environ['Windir'];
        windir = windir.lower()
        windrive_letter = windir[0];
        system_vol = "\\\\.\\"+windrive_letter+":"

        self.makeSystemVolumeBootable()

        Vss = VssThruVshadow.VssThruVshadow()
        snapname = Vss.createSnapshot(system_vol)
      
        WinVol = WindowsVolume.WindowsVolume(snapname)

        self.rollbackSystemVolumeChanges()
        return WinVol

    def makeSystemVolumeBootable(self):
        originalwindir = os.environ['windir']
        windrive = originalwindir.split("\\")[0] #get C: substring
        bootdir = windrive+"\\Boot"
        
        #TODO: note we may need some cleanup management after all theese copies

        #TODO: log
        #TODO: set right permissions to it
        if (os.path.exists(windrive+"\\bootmgr")) == False:
            shutil.copy2(originalwindir + "\\Boot\\PCAT\\bootmgr", windrive+"\\bootmgr")
            self.__filesToDelete.add(windrive+"\\bootmgr")

        if (os.path.exists(bootdir) and os.path.exists(bootdir+"\\BCD")):
            return
        else:
            if os.path.exists(bootdir) == False:
                #create a new one here
                try:
                    shutil.copytree(originalwindir + "\\Boot\\PCAT" , bootdir)
                    self.__filesToDelete.add(bootdir)
                except:
                    #TODO log error
                    return

            # we create an empty BCD so it'll be altered after the transition
            bcdfile = open(bootdir+"\\BCD", "w")
            nullbytes = bytearray(64*1024*1024)
            bcdfile.write(nullbytes)


    def rollbackSystemVolumeChanges(self):
        for file in  self.__filesToDelete:
            shutil.rmtree(file , True , None)
            #TODO: log failures

    def createVhdTransferTarget(self , path , size , adjustOptions):
        logging.debug("Creating VHD transfer target")
        self.__vhd = WindowsVhdMedia.WindowsVhdMedia(path, size+100*1024*1024)
        self.__vhd.open()
          
        datatransfer = WindowsDeviceDataTransferProto.WindowsDeviceDataTransferProto(self.__vhd.getWindowsDevicePath(), self.__vhd.getWindowsDiskNumber())
        parser = WindowsDiskParser.WindowsDiskParser(datatransfer , adjustOptions.getNewMbrId())
        return parser.createTransferTarget(size)
        
    def createSystemAdjustOptions(self):
        return WindowsSystemAdjustOptions.WindowsSystemAdjustOptions()

    def getVersion(self):
        return Windows.Win2008R2