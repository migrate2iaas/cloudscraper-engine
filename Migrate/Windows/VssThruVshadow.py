
import VSS
import sys
import os
import win32api
import MigrateExceptions
import subprocess
import re

#NOt used. see VSSthruVshadow
class VssThruVshadow(VSS.VSS):
    """Mnaages VSS thru vshadow.exe CLI"""

    def __init__(self):
        # locate arch, move to some common Win code
        machine_arch = None
        if os.environ.get("PROCESSOR_ARCHITECTURE") == "AMD64" or os.environ.get("PROCESSOR_ARCHITEW6432") == "AMD64":
            machine_arch = "x86_64"
        else:
            if os.environ.get("PROCESSOR_ARCHITECTURE")=="x86" and os.environ.get("PROCESSOR_ARCHITEW6432") == None:
                machine_arch = "i386"

        self.__machineArch = machine_arch
        (major, minor , build, platform , version) = win32api.GetVersionEx()
        self.__winMajor = major
        self.__winMinor = minor
        
        if self.__machineArch == None:
            raise MigrateExceptions.PropertyNotInitialized("__machineArch" , "Possibly unsupported arch (Itanium)")
     
        path = "vss\\vshadow\\bin\\"+self.__machineArch+"\\"+str(self.__winMajor)+"."+str(self.__winMinor)
        path = path + "\\vshadow.exe"
        
        fullpath = sys.path[0] + "\\Windows\\" + path
        if os.path.exists(fullpath) == False:
            fullpath = sys.path[0] + "\\.\\" + path
            if os.path.exists(fullpath) == False:
                raise MigrateExceptions.PropertyNotInitialized("__binPath " , "Cannot find vshadow binary on path:" + fullpath)

        self.__binPath = fullpath

        self.__snapshots = dict()

    #returns snapshot name in a way it could be opened by an any system call
    def createSnapshot(self, volumeName):
      
        output = subprocess.check_output(self.__binPath + " -p " + volumeName , shell=True);
        match = re.search('Shadow copy device name: ([\\\\][^\r\n ]+)',output)
        if match == None:
            #TODO: log
            raise IOError
        devname = match.group(1)
        
        match = re.search('SNAPSHOT ID = ([{][^\n\r ]+)',output)
        if match == None:
            #TODO: log
            raise IOError
        snapname = match.group(1)
        # make it openable
        devname = devname.replace("\\\\?\\GLOBALROOT\\Device", "\\\\.").lower()
        
        self.__snapshots[devname] = snapname

        return devname

    #deletes the snapshot
    def deleteSnapshot(self, devName):
      
        snapname = self.__snapshots[devName]

        if snapname == None:
            raise KeyError;

        output = subprocess.check_output(self.__binPath + " -ds=" + snapname , shell=True);

        self.__snapshots[devName] = None

        return



