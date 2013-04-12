# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------
import sys

sys.path.append('.\Windows')
sys.path.append('.\Amazon')

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
        return "VHD"
    
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

    def getCloudStorage(self):
        return "feofftestfullmigrate123uswest"

    def getCloudUser(self):
        return 'AKIAIY2X62QVIHOPEFEQ'
    
    def getCloudPass(self):
        return 'fD2ZMGUPTkCdIb8BusZsSp5saB9kNQxuG0ITA8YB'
    
    def getNewSystemSize(self):
        return self.__systemImageSize

    def getTargetCloud(self):
        return "EC2"

    def getArch(self):
        return "x86_64"

    def getZone(self):
        return "us-west-1a"

    def getRegion(self):
        return "us-west-1"

    def getLocalDiskFile(self):
        return self.getSystemImagePath()


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
