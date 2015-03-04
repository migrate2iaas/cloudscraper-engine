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
import time
import logging
import subprocess


import win32api
import win32con
import win32security
import win32file

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
        # TODO: move all viostor related stuff elsewhere
        self.__bootDriverName = "viostor.sys"
        self.__bootDriverInf = "viostor.inf"

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
            virtiodir = virtiodir + "\\WIN7" # pnputil doesn't work for win8 drivers so we load win7 ones

        if WindowsSystemInfo.WindowsSystemInfo().getSystemArcheticture() == WindowsSystemInfo.WindowsSystemInfo.Archx8664:
            virtiodir = virtiodir + "\\AMD64"
        else:
            virtiodir = virtiodir + "\\X86"
        return virtiodir

    def __executePreprocess(self):
        """executes preprocess bat"""
        windir = os.environ['Windir'] 
        windir = windir.lower()
        windrive_letter = windir[0] 

        batname = windrive_letter+":\\"+Windows.adjustServiceDir+"\\preprocess.cmd"
        cmd =  subprocess.Popen(['cmd', '/C', batname ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        returncode = cmd.wait()
        if cmd.stdout:
            logging.debug(cmd.stdout.read())
        logging.debug(batname + " returned " + str(returncode))
        if returncode <> 0:
            logging.warn("! Failed to add Virtio drivers to the running system!")
        #    raise Exception("Cannot execute PnPutil to add drivers to the store!")


    # volume should be in "\\.\X:" form
    def getDataBackupSource(self , volume):
        
        snapname = self.__vss.createSnapshot(volume)
      
        WinVol = WindowsVolume.WindowsVolume(snapname)

        return WinVol
        

    def getSystemDataBackupSource(self):
        logging.debug("Getting the system backup source") 
        windir = os.environ['Windir'] 
        windir = windir.lower()
        windrive_letter = windir[0] 
        system_vol = "\\\\.\\"+windrive_letter+":"

        try:
            self.__makeSystemVolumeBootable()

            #TODO: add extra config flags for this! 
            #TODO: make use of this singleton class more consistent
            self.__copyExtraFiles()
            self.__prepareRegistry()
            self.__executePreprocess()
            

            snapname = self.__vss.createSnapshot(system_vol)
            WinVol = WindowsVolume.WindowsVolume(snapname)
        except Exception as e:
            logging.error("!!!ERROR: Cannot prepare source system!")
            raise
        finally:
            self.__rollbackSystemVolumeChanges()
        return WinVol

    #frees the snapshot
    def freeDataBackupSource(self , vol):
        
        volumename = vol.getVolumeName()
        self.__vss.deleteSnapshot(volumename)

    def __copyExtraFiles(self):
        """copies extra files: virtio drivers and autoadjust service """
        # copies whole virtio to Windows\System32\drivers
        originalwindir = os.environ['windir']
        windrive = originalwindir.split("\\")[0] #get C: substring
        wininstall = os.getenv('SystemRoot', windrive+"\\windows")
        system32_folder = wininstall+"\\system32"
        # to check whether we are executed under 64-bit and WOW64 emulates system32 for us , see http://www.xyplorer.com/faq-topic.php?id=wow64
        if os.path.exists(wininstall + "\\SysWOW64"):
            logging.debug("Writing to sysnative instead of WOW64-emulated system32")
            system32_folder = wininstall + "\\sysnative"
            if os.path.exists(system32_folder) == False:
               logging.error("!!!ERROR: Please install the MS KB 942589 http://support.microsoft.com/kb/942589 to run on Win2003 64-bit servers!")
        
        #TODO: make more modular early boot code
        # here we add early boot driver to system32\drivers checking it not exists
        # if it exists, renaming for the short period of time
        # note: the deletion is executed in cleanup before the renaming
        files_to_copy = [system32_folder + "\\drivers\\"+self.__bootDriverName, wininstall + "\\inf\\"+ self.__bootDriverInf ]

        if self.__windowsVersion >= WindowsSystemInfo.WindowsSystemInfo.Win2012:
            # don't copy files for Win2012, they seem to mess with the system
            files_to_copy = []
        
        for conflicting_file in files_to_copy:
            if os.path.exists(conflicting_file):
                logging.debug("Virt-IO file " + conflicting_file + " detected on the machine!")
                tempname = conflicting_file+"-renamed"+str(int(time.time()))
                logging.debug("Renaming it to " + tempname + " temporary!")
                os.rename(conflicting_file, tempname)
                self.__filesToRename[conflicting_file] = tempname

            shutil.copy(self.__virtIoDir+"\\"+os.path.basename(conflicting_file) , conflicting_file)
            self.__filesToDelete.add(conflicting_file)

        # copies adjust service
        shutil.copytree(self.__adjustSvcDir , windrive + "\\" + Windows.adjustServiceDir)
        self.__filesToDelete.add(windrive + "\\" + Windows.adjustServiceDir)

        # copies virtio dir
        logging.debug("Copy virtio dir");
        virtio_copy_path = windrive + "\\" + Windows.adjustServiceDir+"\\virtio"; #wininstall + "\\system32\\drivers\\virtio"
        shutil.copytree(self.__virtIoDir , virtio_copy_path)


    def __makeSystemVolumeBootable(self):
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
    

    def __prepareRegistry(self):
        self.__injectVirtIo()
        self.__injectPostprocess(Windows.adjustRelSvcDir)
        return

    def __rollbackSystemVolumeChanges(self):
        for file in  self.__filesToDelete:
            logging.debug("Deleting temporary file " + file)       
            shutil.rmtree(file , True , None)
            if os.path.exists(file) and os.path.isdir(file):
                os.rmdir(file)
            if os.path.exists(file) and os.path.isfile(file):
                os.remove(file)
            #TODO: log failures
        for (oldfile , newfile) in self.__filesToRename.items():
            logging.debug("Renaming " + newfile + " file to " + oldfile)
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


    def __mergeReg(self , newreg):
        cmd =  subprocess.Popen(["reg" , "import" , newreg], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        returncode = cmd.wait()
        output = cmd.stdout.read()
        if cmd.stdout:
            logging.debug(output)
        logging.debug("reg returned " + str(returncode))
        if returncode <> 0:
            logging.warn("! Failed to add Virtio drivers to the registry!")
        return

    def __injectViostor2012Package(self , keyname):
        hivekeyname = "SYSTEM"
        regfilepath = Windows.Windows.virtRelIoDir + "\\viostor2012.reg"
        regfile = open(regfilepath , "r")
        data = regfile.read()
        regfile.close()
        data = data.replace("<VIOSTOR>",keyname).replace("<SYSHIVE>",hivekeyname)
        logging.debug("Inserting registry \n" + data)
        filename = os.tempnam("viostor2012") + ".reg"
        newfile = open(filename , "w")
        newfile.write(data)
        newfile.close()
        self.__mergeReg(filename)

    def __injectViostor2012(self):
        hivekeyname = "SYSTEM"
        driverskey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\DriverDatabase\\DriverPackages" , 0 , win32con.KEY_ALL_ACCESS )
        infkeys = win32api.RegEnumKeyEx(driverskey)
        for (keyname, reserved, classname, modtime) in infkeys:
            if "viostor.inf" in keyname:
                logging.debug("Adding dirver package info to " + hivekeyname+"\\DriverDatabase\\DriverPackages\\" +  keyname + " . Should  be run from the system account")
                self.__injectViostor2012Package(keyname)

        driverskey.close()
        

    def __injectVirtIo(self):
        """
        
        injects virtio drivers adding them to the corresponding hives
        
        """
        
        logging.info(">>>> Adding virt-io drivers to the running system")

        root_virtio = Windows.virtRelIoDir # + virtio_path
        virtiodir="%SystemRoot%\\system32\\drivers\\virtio"

        # 2) open reg file, replace data with corresponding values, inject it to our hive
        # the injection reg file is found in corresponding resource directory
        #newreg = self.prepareRegFile(hivekeyname , "ControlSet00"+str(currentcontrolset) ,  ,  virtiodir , virtiodir )
        self.__mergeReg(root_virtio + "\\virtio.reg")     

        if self.__windowsVersion >= WindowsSystemInfo.WindowsSystemInfo.Win2012:
            self.__injectViostor2012()

        return True

    def __injectPostprocess(self , postprocess_service_dir):
        """
        inject postprocess service using the corresponding 
        Note: it's assumed that the service is copied to "C:\\CloudscraperBootAdjust" (see Windows.Windows.adjustServiceDir)
        """
        postprocess_service_path = postprocess_service_dir + "\\nssm"

        if WindowsSystemInfo.WindowsSystemInfo().getSystemArcheticture() == WindowsSystemInfo.WindowsSystemInfo.Archx8664:
            postprocess_service_path = postprocess_service_path + "\\win64"
        else:
            postprocess_service_path = postprocess_service_path + "\\win32"

        postprocess_service_exe = postprocess_service_path + "\\nssm.exe"
        postprocess_service_reg = postprocess_service_path + "\\nssm.reg"

       # newreg = self.prepareRegFile(hivekeyname , "ControlSet00"+str(currentcontrolset) , postprocess_service_reg)
        self.__mergeReg(postprocess_service_reg)   


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

    