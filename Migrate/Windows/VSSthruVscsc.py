import VSS
import sys
import os
import win32api
import MigrateExceptions
import subprocess
import re

#NOt used. see VSSthruVshadow
class VSSthruVscsc(VSS.VSS):
    """Mnaages VSS thru Vscsc.exe CLI"""

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
        
        if self.__machineArch != i386:
            path = ".\\Windows\\vscs\\bin"+self.__machineArch+"\\6.0";
        else:
            if self.__winMajor >= 6:
                path = ".\\Windows\\vscs\\bin"+self.__machineArch+"\\6.0";
            else:
                path = ".\\Windows\\vscs\\bin"+self.__machineArch+"\\"+str(self.__winMajor)+"."+str(self.__winMinor)
        
        if self.__machineArch == None:
            raise MigrateExceptions.PropertyNotInitialized("__machineArch" , "Possibly unsupported arch (Itanium)")
        path = "";

        path = path + "\\vscsc.exe"

        if os.path.exists(path) == False:
            raise MigrateExceptions.PropertyNotInitialized("__binPath " , "Cannot find Vscs binary on path:" + path)

        self.__binPath = path

        self.__Snapshots = dict()


    def createSnapshot(self, volumeName):
      
        output = subprocess.check_output(self.__binPath + " " + volumeName , shell=True);
        match = re.search('Shadow copy device name: ([\\\\][^\n ]+)',output)
        if match == None:
            #TODO: log
            raise IOError
        devname = match.group(1)
        
        match = re.search('SNAPSHOT ID = ([{][^ ]+)',output)
        if match == None:
            #TODO: log
            raise IOError
        snapname = match.group(1)
        

        return devname

    def deleteSnapshot(self, snapName):
      
        output = subprocess.check_output(self.__binPath + " " + volumeName , shell=True);
        match = re.search('Shadow copy device name: ([\\\\][^\n ]+)',output)
        if match == None:
            #TODO: log
            raise IOError

        volumename = match.group(1)
        return volumename


