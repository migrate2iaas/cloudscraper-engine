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
        logging.debug("Getting the system backup source");
        windir = os.environ['Windir'];
        windir = windir.lower()
        windrive_letter = windir[0];
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
            logging.debug("Bootmgr not found in Windows root , copying it there");      
            shutil.copy2(originalwindir + "\\Boot\\PCAT\\bootmgr", windrive+"\\bootmgr")
            self.__filesToDelete.add(windrive+"\\bootmgr")

        if (os.path.exists(bootdir) and os.path.exists(bootdir+"\\BCD")):
            return
        else:
            if os.path.exists(bootdir) == False:
                #create a new one here
                try:
                    logging.debug("Creating a bootdir");      
                    shutil.copytree(originalwindir + "\\Boot\\PCAT" , bootdir)
                    self.__filesToDelete.add(bootdir)
                except:
                    logging.error("Cannot create bootdir");      
                    #TODO log error
                    return

            logging.debug("Creating an empty BCD store");      
            # we create an empty BCD so it'll be altered after the transition
            bcdfile = open(bootdir+"\\BCD", "w")
            nullbytes = bytearray(64*1024*1024)
            bcdfile.write(nullbytes)


    def rollbackSystemVolumeChanges(self):
        for file in  self.__filesToDelete:
            logging.debug("Deleting temporary file" + file);      
            shutil.rmtree(file , True , None)
            #TODO: log failures

    def createVhdTransferTarget(self , path , size , adjustOptions):
        logging.debug("Creating VHD transfer target")
        gigabyte = 1024*1024*1024
        vhd = WindowsVhdMedia.WindowsVhdMedia(path, size+gigabyte)
        vhd.open()
          
        #really this device data transfer proto is not used, it seems to be a connector between raw media and methods to access its contents
        datatransfer = WindowsDeviceDataTransferProto.WindowsDeviceDataTransferProto(vhd.getWindowsDevicePath(), vhd.getWindowsDiskNumber() , vhd)
        parser = WindowsDiskParser.WindowsDiskParser(datatransfer , adjustOptions.getNewMbrId())
        return parser.createTransferTarget(size)
        
    def createSystemAdjustOptions(self):
        return WindowsSystemAdjustOptions.WindowsSystemAdjustOptions()

    def getVersion(self):
        return WindowsSystemInfo.WindowsSystemInfo().getKernelVersion()

    def getSystemInfo(self):
        return WindowsSystemInfo.WindowsSystemInfo()
