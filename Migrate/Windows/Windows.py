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

    # path on C:\ where the service is stored
    adjustServiceDir = "CloudscraperBootAdjust"
    
    # pathes to resource directories
    virtRelIoDir = "..\\resources\\virtio\\1.65"  #TODO: make customizeable
    adjustRelSvcDir = "..\\resources\\CloudscraperBootAdjust"

    # TODO: some init configs could be read here, e.g. Windows configs
    def __init__(self):
        self.__filesToDelete = set()
        self.__filesToRename = dict() # key is old name and value is a new one
        self.__vss = VssThruVshadow.VssThruVshadow()
        #note: better to load from conf
        self.__halList = ["halacpi.dll", "halmacpi.dll", "halaacpi.dll"]
        self.__copyVirtIo = True
        self.__adjustSvcDir = Windows.adjustRelSvcDir
        self.__windowsVersion = self.getVersion()
        self.__virtIoDir = self.__setVirtIoSourceDir()
        self.__bootDriverName = "viostor.sys"

        return
    
    def __setVirtIoSourceDir(self):
        virtiodir = Windows.virtRelIoDir
       
        if self.__windowsVersion == WindowsSystemInfo.WindowsSystemInfo.Win2003:
            virtiodir = virtiodir + "\\WNET"
        if self.__windowsVersion == WindowsSystemInfo.WindowsSystemInfo.Win2008:
            virtiodir = virtiodir + "\\WLH"
        if self.__windowsVersion == WindowsSystemInfo.WindowsSystemInfo.Win2008R2:
            virtiodir = virtiodir + "\\WIN7"
        if self.__windowsVersion >= WindowsSystemInfo.WindowsSystemInfo.Win2012:
            virtiodir = virtiodir + "\\WIN8"

        if WindowsSystemInfo.WindowsSystemInfo().getSystemArcheticture() == WindowsSystemInfo.WindowsSystemInfo.Archx8664:
            virtiodir = virtiodir + "\\AMD64"
        else:
            virtiodir = virtiodir + "\\X86"
        return virtiodir

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
        self.copyExtraFiles()

        snapname = self.__vss.createSnapshot(system_vol)
      
        WinVol = WindowsVolume.WindowsVolume(snapname)

        self.rollbackSystemVolumeChanges()
        return WinVol

    #frees the snapshot
    def freeDataBackupSource(self , vol):
        
        volumename = vol.getVolumeName()
        self.__vss.deleteSnapshot(volumename)

    def copyExtraFiles(self):
        """copies extra files: virtio drivers and autoadjust service """
        # copies whole virtio to Windows\System32\drivers
        originalwindir = os.environ['windir']
        windrive = originalwindir.split("\\")[0] #get C: substring
        wininstall = os.getenv('SystemRoot', windrive+"\\windows")
        # locate virt-io 
        shutil.copytree(self.__virtIoDir , wininstall + "\\system32\\drivers\\virtio")
        self.__filesToDelete.add(wininstall + "\\system32\\drivers\\virtio")
        
        #TODO: make more modular early boot code
        # here we add early boot driver to system32\drivers checking it not exists
        # if it exists, renaming for the short period of time
        # note: the deletion is executed in cleanup before the renaming
        bootdriver_name = self.__bootDriverName
        conflicting_file = wininstall + "\\system32\\drivers\\"+bootdriver_name
        if os.path.exists(conflicting_file):
            logging.debug("Virt-IO vioscsi driver detected on the machine!")
            logging.debug("Renaming it to add target Virt-IO driver")
            os.rename(conflicting_file, conflicting_file+"-renamed")
            self.__filesToRename[conflicting_file] = conflicting_file+"-renamed"

        shutil.copy(wininstall + "\\system32\\drivers\\virtio\\"+bootdriver_name , wininstall + "\\system32\\drivers")
        self.__filesToDelete.add(conflicting_file)

        # copies adjust service
        shutil.copytree(self.__adjustSvcDir , windrive + "\\" + Windows.adjustServiceDir)
        self.__filesToDelete.add(windrive + "\\" + Windows.adjustServiceDir)


    def makeSystemVolumeBootable(self):
        originalwindir = os.environ['windir']
        windrive = originalwindir.split("\\")[0] #get C: substring
        wininstall = os.getenv('SystemRoot', windrive+"\\windows")

        # Amazon hack. 
        # These files are needed to be renamed in order amazon conversion script (C:\EC2VmConversion\install.bat) won't hang
        conflicting_files = [wininstall+"\\setup\\scripts\\SetupComplete.cmd", \
                             windrive+"\\Program Files\\Citrix\\XenTools\\XenDpriv.exe.Config" , windrive+"\\Program Files\\Citrix\\XenTools\\XenGuestAgent.exe.Config" , \
                             windrive+"\\Program Files(x86)\\Citrix\\XenTools\\XenDpriv.exe.Config" , windrive+"\\Program Files(x86)\\Citrix\\XenTools\\XenGuestAgent.exe.Config" ]
        for conflicting_file in conflicting_files:
            if os.path.exists(conflicting_file):
                self.__filesToRename[conflicting_file] = conflicting_file + "-renamed"
                os.rename(conflicting_file , self.__filesToRename[conflicting_file])


        #manage bcd
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
            if self.getSystemInfo().getSystemArcheticture() != WindowsSystemInfo.WindowsSystemInfo.Archi386:
                logging.debug("No need to adjust HAL for x64 version")  
                return
            #compare HALs,
            self.unpackHal() 
            sys32dir = originalwindir+"\\system32"
            currenthal = sys32dir+"\\hal.dll"
            good_hal_already = False
            for hal in self.__halList:
                targethal = sys32dir+"\\"+hal
                #TODO: check exisits
                if os.path.exists(targethal):
                    if filecmp.cmp(targethal , currenthal , 0):
                        logging.info("The HAL ( " + hal + " ) doesn't need to be virtualized, skipping")
                        good_hal_already = True
            
            if good_hal_already == False:
                logging.error("!!!ERROR Non-standard HAL are not supported. Please, make a P2V migration first!")
            
            #this code could be used to support non-standard hals but could damage boot.ini
            #boot_ini = windrive+"\\boot.ini"
            #file = open(boot_ini , "r")
            #data = file.read()
            #file.close()
            

    def unpackHal(self):
        import cabinet
        originalwindir = os.environ['windir']
        sys32dir = originalwindir+"\\system32"

        filerepopath = originalwindir+"\\Driver Cache"
        if self.getSystemInfo().getSystemArcheticture() == WindowsSystemInfo.WindowsSystemInfo.Archi386:
            filerepopath = filerepopath + "\\i386"
        else:
            filerepopath = filerepopath + "\\amd64"
        
        #we seek for the latest cab
        max_modtime = 0
        cabs = os.listdir(filerepopath) 
        for dirent in cabs:
            if dirent.endswith(".cab"):
                cabpath = filerepopath+"\\"+dirent
                modtime = os.stat(cabpath).st_mtime
                if modtime >= max_modtime:
                    max_modtime = modtime
                else:
                    continue
                logging.info("Extracting HAL from " + cabpath)
                cab = cabinet.CabinetFile(cabpath)
                available_hals = [val for val in cab.namelist() if val in set(self.__halList)] #looks crazy but it is how interesction works
                for hal in available_hals:
                    logging.debug("Extracting " + hal)
                    cab.extract(sys32dir , [hal])
               
        return True

    def rollbackSystemVolumeChanges(self):
        for file in  self.__filesToDelete:
            logging.debug("Deleting temporary file" + file)       
            shutil.rmtree(file , True , None)
            if os.path.exists(file) and os.path.isdir(file):
                os.rmdir(file)
            #TODO: log failures
        for (oldfile , newfile) in self.__filesToRename.items():
            os.rename(newfile , oldfile)

        
    def createSystemAdjustOptions(self , config = dict()):
        # we should get specific configs here to generate the correct config
        options = WindowsSystemAdjustOptions.WindowsSystemAdjustOptions(self.getVersion() < WindowsSystemInfo.WindowsSystemInfo.Win2008R2 , \
            self.getVersion() >= WindowsSystemInfo.WindowsSystemInfo.Win2008)
        # TODO: here we should check windows version and add some configs from pre-build configs
        options.loadConfig(config)
        if (self.getVersion() < WindowsSystemInfo.WindowsSystemInfo.Win2008):
            options.setSysDiskType(options.diskAta)

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

    