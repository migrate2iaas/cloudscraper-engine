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

import filecmp
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

        if self.getVersion() >= WindowsSystemInfo.WindowsSystemInfo.Win2008:

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
        else:
            #compare HALs,
            self.unpackHal() 
            sys32dir = originalwindir+"\\system32"
            targethal = sys32dir+"\\halacpi.dll"
            targethal2 = sys32dir+"\\halmacpi.dll"
            currenthal = sys32dir+"\\hal.dll"
            if filecmp.cmp(targethal , currenthal , 0) or filecmp.cmp(targethal2 , currenthal , 0):
                logging.info("The HAL doesn't need to be virtualized, skipping")
            else:
                logging.error("!ERROR Non-standard HAL are not supported. Please, make a P2V migration first!")
                #boot_ini = windrive+"\\boot.ini"
                #file = open(boot_ini , "r")
                #data = file.read()
                #file.close()


    def unpackHal(self):
        import cabinet
        originalwindir = os.environ['windir']
        sys32dir = originalwindir+"\\system32"
        targethal = "halacpi.dll"
        targethal2 = "halmacpi.dll"
        if os.path.exists(sys32dir + "\\" +targethal) == False:
            return True

        filerepopath = originalwindir+"\\Driver Cache"
        if self.getSystemInfo().getSystemArcheticture() == WindowsSystemInfo.WindowsSystemInfo.Archi386:
            filerepopath = filerepopath + "\\i386"
        else:
            filerepopath = filerepopath + "\\amd64"
        
        cabs = os.listdir(filerepopath) 
        for dirname in cabs:
            if dirname.endswith(".cab"):
                logging.info("Extracting HAL from " + filerepopath+"\\"+dirname)
                cab = cabinet.CabinetFile(filerepopath+"\\"+dirname)
                if targethal in cab.namelist():
                    cab.extract(sys32dir , [targethal])
                if targethal2 in cab.namelist():
                    cab.extract(sys32dir , [targethal2])
                return True

        return False

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
        
    def createSystemAdjustOptions(self , config = dict()):
        # we should get specific configs here to generate the correct config
        options = WindowsSystemAdjustOptions.WindowsSystemAdjustOptions(self.getVersion() < WindowsSystemInfo.WindowsSystemInfo.Win2008R2)
        # TODO: here we should check windows version and add some configs from pre-build configs
        options.loadConfig(config)
        return options

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

    