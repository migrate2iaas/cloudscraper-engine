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
        systemdisk = "/dev/sda"

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