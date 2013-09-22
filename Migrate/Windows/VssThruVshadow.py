# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys
import os
sys.path.append(sys.path[0]+'\\..')
from MigrateExceptions import FileException
import VSS
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
import MigrateExceptions
import subprocess
import re
import Windows
import struct


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

        volname = self.mountSnapshot(devname);
        
        logging.debug("Saving %s snapshot for device \'%s\' mounted on \'%s\'" , snapname , devname , volname);
        self.__snapshots[volname] = snapname
        logging.debug(str(self.__snapshots));

        return volname

    def mountSnapshot(self , devname):
        """mounts snapshot on a path accessible by Windows user-mode"""
        volname = devname.replace("\\\\?\\GLOBALROOT\\Device", "\\\\.").lower()
        #Win2008 VSS creates symlink themself. 
        #TODO: make dynamic checks if symlinks exists
        if (self.__winMajor > 5): 
            return volname

        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        #openning Windows mount manager to create a link
        filename = "\\\\.\\MountPointManager" # MOUNTMGR_DEVICE_NAME
        try:
            self.__hfile = win32file.CreateFile( filename, win32con.GENERIC_READ|  win32con.GENERIC_WRITE | ntsecuritycon.FILE_READ_ATTRIBUTES | ntsecuritycon.FILE_WRITE_ATTRIBUTES, win32con. FILE_SHARE_READ|win32con.FILE_SHARE_WRITE, secur_att,   win32con.OPEN_ALWAYS, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        except Exception as ex:
            raise FileException(filename , ex)

        symlink = unicode(devname.replace("\\\\?\\GLOBALROOT\\Device", "\\DosDevices"))
        kernellink = unicode(devname.replace("\\\\?\\GLOBALROOT\\Device", "\\Device"))
        # typedef struct _MOUNTMGR_CREATE_POINT_INPUT {
        # USHORT SymbolicLinkNameOffset;
        # USHORT SymbolicLinkNameLength;
        # USHORT DeviceNameOffset;
        # USHORT DeviceNameLength;
        #} MOUNTMGR_CREATE_POINT_INPUT, *PMOUNTMGR_CREATE_POINT_INPUT;
        #create symbolic link
        output_buffer_size = 256 
        struct_sizeof = 4*2
        IOCTL_MOUNTMGR_CREATE_POINT = 0x6dc000
        symlinklen = len(symlink)*2
        kernelllinklen = len(kernellink)*2
        input_buffer = struct.pack("=HHHH", struct_sizeof , symlinklen ,   struct_sizeof+symlinklen,  kernelllinklen)\
            + bytearray(symlink, encoding = "utf-16LE") + bytearray(kernellink , encoding = "utf-16LE")
        try:
            win32file.DeviceIoControl(self.__hfile, IOCTL_MOUNTMGR_CREATE_POINT, input_buffer, 0, None )
        except Exception as ex:
            logging.error("!!!ERROR Failed to mount volume snapshot so it could be accessed by the program")
            raise FileException(filename , ex)

        return volname

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



