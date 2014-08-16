"""
The Linux python module declaration
~~~~~~~~~~~~~~~~~

This module provides Linux singleon class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback




import filecmp
import unittest
import shutil
import logging

import LinuxAdjustOptions
import LinuxBackupSource
import LinuxSystemInfo
import LinuxVolumeInfo
import LinuxBlockDevice
import LinuxBackupAdjust

import re
from subprocess import *

class Linux(object):
    
    def createSystemAdjustOptions(self):

         # we should get specific configs here to generate the correct config
        options = WindowsSystemAdjustOptions.WindowsSystemAdjustOptions(self.getVersion() < WindowsSystemInfo.WindowsSystemInfo.Win2008R2 , \
            self.getVersion() >= WindowsSystemInfo.WindowsSystemInfo.Win2008)
        # TODO: here we should check windows version and add some configs from pre-build configs
        options.loadConfig(config)
        if (self.getVersion() < WindowsSystemInfo.WindowsSystemInfo.Win2008):
            options.setSysDiskType(options.diskAta)

        return options

    def getSystemDataBackupSource(self):
        logging.debug("Getting the system backup source") 
        
        #get system disk
        systemdisk = self.getSystemDriveName()

        lindisk = LinuxBlockDevice.LinuxBlockDevice(systemdisk)

        return lindisk
    
    def getDataBackupSource(self , volume):
        logging.debug("Getting data backup source") 
        
        #get system disk
        systemdisk = volume

        lindisk = LinuxBlockDevice.LinuxBlockDevice(systemdisk)

        return lindisk

    def getSystemInfo(self):
        return LinuxSystemInfo.LinuxSystemInfo()

    def findDeviceForPath(self , path):
        p1 = Popen(["df" , path], stdout=PIPE)
        output = p1.communicate()[0]
        lastline = output.split("\n")[1]
        voldev = lastline[:lastline.find(" ")]
        return voldev

    def __findLvmDev(self , volgroup):
        p1 = Popen(["lvdisplay" , "-m", volgroup], stdout=PIPE)
        output = p1.communicate()[0]
        
        if str(output).count("Physical volume") > 1:
            logging.error("!!!ERROR: LVM config is too complex to parse!")
            raise LookupError()

        match = re.search( "Physical volume ([^\n]*)", output )
        if match == None:
            logging.error("!!!ERROR: Couldn't parse LVM config!")
            raise LookupError()

        volume = match.group(1)
        return volume.strip()

    def getSystemDriveName(self):
        rootdev = self.findDeviceForPath("/")
        bootdev = self.findDeviceForPath("/boot")

        
        logging.info("The root device is " + rootdev);
        logging.info("The boot device is " + bootdev);

        # try to see where it resides. it's possible to be an lvm drive
        if rootdev.count("mapper/VolGroup-") > 0: 
             volgroup = str(rootdev).replace('mapper/VolGroup-', 'VolGroup/')
             rootdev = self.__findLvmDev(volgroup)
             logging.info("LVM " + volgroup + " resides on " + rootdev);

        rootdrive = rootdev[:-1]
        bootdrive = bootdev[:-1]


        if rootdrive != bootdrive:
            logging.warn("!Root and boot drives are on different physical disks. The configuration could lead to boot failures.")
       
        #substract the last number making /dev/sda from /dev/sda1. 
        # In the current impl we do full disk backup
        return rootdrive