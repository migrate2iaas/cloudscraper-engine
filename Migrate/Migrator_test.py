
import sys

sys.path.append('.\Windows')

import unittest
import WindowsVolume
import WindowsBackupSource
import AdjustedBackupSource
import BackupAdjust
import Migrator
import SystemAdjustOptions

import Windows

import logging


class TestMigrateOptions(object):
    
    def __init__(self , vhdpath, sysimagesize, disktype):
        self.__vhdPath = vhdpath
        self.__systemImageSize = sysimagesize
        self.__diskType = disktype
        return

    def getHostOs(self):
        return "Windows"

    def getImageType(self):
        return "vhd"
    
    def getImagePlacement(self):
        return "local"

    def getSystemImagePath(self):
        return self.__vhdPath + "system.vhd"

    def getSystemImageSize(self):
        return self.__systemImageSize

    def getSystemConfig(self):
        return None

    def getSystemDiskType(self):
        return self.__diskType

class Migrator_test(unittest.TestCase):
    """Migrator tests"""
    
    def setUp(self):
        size = Windows.Windows().getSystemInfo().getSystemVolumeInfo().getSize()
        options = TestMigrateOptions("E:\\" , size , SystemAdjustOptions.SystemAdjustOptions.diskUnknown) 
        self.__migrator = Migrator.Migrator(options)

        logging.basicConfig(format='%(asctime)s %(message)s' , filename='migrator.log',level=logging.DEBUG)
        

    def test_migrator(self):
        logging.info("Migrator test started")
        self.__migrator.runFullScenario()
        logging.info("Migrator test ended")

if __name__ == '__main__':
    unittest.main()
