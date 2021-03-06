# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys


sys.path.append('.\Windows')

import unittest
import WindowsVolume
import WindowsBackupSource
import AdjustedBackupSource
import BackupAdjust
import WindowsVolumeTransferTarget
import WindowsDiskParser
import WindowsVhdMedia
import WindowsDeviceDataTransferProto
import time
import logging

class WindowsDiskParaser_test(unittest.TestCase):
     #TODO: make more sophisticated config\test reading data from some config. dunno

    def setUp(self):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        now = time.localtime()
        self.__vhd = WindowsVhdMedia.WindowsVhdMedia("E:\\vhdtest"+str(now)+".vhd", 200*1024*1024)
     
        

    def test_part(self):
        logging.debug("Not used")
        return 
        self.__vhd.open()
        datatransfer = WindowsDeviceDataTransferProto.WindowsDeviceDataTransferProto(self.__vhd.getWindowsDevicePath(), self.__vhd.getWindowsDiskNumber() , self.__vhd)
        parser = WindowsDiskParser.WindowsDiskParser(datatransfer, 123456)
        parser.createTransferTarget(128*1024*1024)
        parser.createTransferTarget(64*1024*1024)


if __name__ == '__main__':
    unittest.main()

