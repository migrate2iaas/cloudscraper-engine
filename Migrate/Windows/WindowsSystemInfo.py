# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys
sys.path.append(sys.path[0]+'\\..')

import subprocess
import re


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
import VolumeInfo
import os

import WindowsVolumeInfo
import SystemInfo

#TODO; inherit from the common system info class
class WindowsSystemInfo(SystemInfo.SystemInfo):
    """System Info of Windows system"""

    Win2000 = 0x50
    WinXP = 0x51
    Win2003 = 0x52
    Win2008 = 0x60
    Win2008R2 = 0x61
    Win2012 = 0x62

   

    def __init__(self):

        super(WindowsSystemInfo, self).__init__()

        machine_arch = self.ArchUnknown
        if os.environ.get("PROCESSOR_ARCHITECTURE") == "AMD64" or os.environ.get("PROCESSOR_ARCHITEW6432") == "AMD64":
            machine_arch = self.Archx8664
        else:
            if os.environ.get("PROCESSOR_ARCHITECTURE")=="x86" and os.environ.get("PROCESSOR_ARCHITEW6432") == None:
                machine_arch = self.Archi386

        self.__machineArch = machine_arch

        (major, minor , build, platform , version , servicepackmajor, servicepackminor , suite, producttype, reserved) = win32api.GetVersionEx(1)
        self.__winMajor = major
        self.__winMinor = minor
        self.__winBuild = build
        self.__winPlatform = platform
        self.__winVersionExtraString = version
        self.__winServicePackMajor = servicepackminor
        self.__winServicePackMinor = servicepackminor
        self.__winOsType = producttype
        self.__winSuite = suite

        return

    # gets arbitary-way formatted string describing the current system
    def getSystemVersionString(self):
        suite = ""
        ostype = "Unknown"
        if self.__winOsType == 1: #VER_NT_WORKSTATION:
            ostype = "Desktop";
        if self.__winOsType == 3: #VER_NT_SERVER:
            ostype = "Server"
        # What to do in this case?
        if self.__winOsType == 2: #VER_NT_DOMAIN_CONTROLLER:
            ostype = "Domain Controller"
        #TODO: convert self.__winSuite to text according to _OSVERSIONINFOEX
        return "Windows "+ ostype + " " + str(self.__winMajor)+"."+str(self.__winMinor)+"."+str(self.__winBuild)+" SP:"+str(self.__winServicePackMajor)+"."+str(self.__winServicePackMinor)+" \"" + self.__winVersionExtraString +"\" Suite mask:" +  hex(self.__winSuite)


    def getKernelVersion(self):
        return int(self.__winMajor * 0x10 + self.__winMinor)

    def getSystemArcheticture(self):
        return self.__machineArch

    #gets system volume info, one where kernel/drivers is situated
    def getSystemVolumeInfo(self):
        originalwindir = os.environ['windir']
        windrive = originalwindir.split("\\")[0] #get C: substring
        return WindowsVolumeInfo.WindowsVolumeInfo(windrive)

    #gets the volume info by its path
    def getVolumeInfo(self , path):
        return WindowsVolumeInfo.WindowsVolumeInfo(path)

    # gets iterable to iterate thru volumes in system
    def getDataVolumesInfo(self):
        #TODO: implement iterator
        return

    def getHardwareInfo(self):
        #TODO: implement
        return

    def getNetworkInfo(self):
        #TODO: implement
        return