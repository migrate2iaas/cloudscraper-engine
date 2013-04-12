# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import VSS
import sys
import os
import win32api
import MigrateExceptions
import subprocess
import re

import logging

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
        
        #NOTE: Volume should have enough space to create shadow storage on it
        #Use to add extra storage on another drive for vss snapshot "vssadmin add shadowstorage /for=<ForVolumeSpec> /on=<OnVolumeSpec> [/maxsize=<MaxSizeSpec>]"

        #TODO: add better retunr. not check output but some subprocess call 
        try:
            output = subprocess.check_output("\"" + self.__binPath + "\"" + " -p " + volumeName , shell=True);
        except subprocess.CalledProcessError as ex:
            logging.error("!!!ERROR: Cannot create Windows volume snapshot")
            logging.error("vshadow failed" + ex.output)
            raise

        match = re.search('Shadow copy device name: ([\\\\][^\r\n ]+)',output)
        if match == None:
            logging.error("!!!ERROR: Cannot create Windows volume snapshot")
            logging.error("Bad vhsadow output! Cannot find shadow copy device name! Output %s" , output)
            raise IOError
        devname = match.group(1)
        
        match = re.search('SNAPSHOT ID = ([{][^\n\r ]+)',output)
        if match == None:
            logging.error("!!!ERROR: Cannot create Windows volume snapshot")
            logging.error("Bad vhsadow output! Cannot find snapshot id! Output %s" , output)
            raise IOError
        snapname = match.group(1)
        # make it openable
        devname = devname.replace("\\\\?\\GLOBALROOT\\Device", "\\\\.").lower()
        
        logging.debug("Saving %s snapshot for device \'%s\'" , snapname , devname);
        self.__snapshots[devname] = snapname
        logging.debug(str(self.__snapshots));

        return devname

    #deletes the snapshot
    def deleteSnapshot(self, devName):
      
        logging.debug("Deleting snapshot for device \'%s\'" , devName);
        logging.debug(str(self.__snapshots));
        snapname = self.__snapshots[devName]

        if snapname == None:
            raise KeyError;
        
        try:
            output = subprocess.check_output("\"" + self.__binPath + "\"" + " -ds=" + snapname , shell=True);
        except subprocess.CalledProcessError as ex:
            logging.error("!!!ERROR: Cannot release Windows volume snapshot")
            logging.error("vshadow failed" + ex.output)
            raise

        self.__snapshots[devName] = None

        return



