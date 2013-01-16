
import sys

sys.path.append('.\Windows')

import unittest
import WindowsVolume
import WindowsBackupSource
import AdjustedBackupSource
import BackupAdjust
import Migrator

import logging


class TestMigrateOptions(object):
    def __init__(self , vhdpath, sysimagesize):
        self.__vhdPath = vhdpath
        self.__systemImageSize = sysimagesize
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

class Migrator_test(unittest.TestCase):
    """Migrator tests"""
    
    def setUp(self):
        options = TestMigrateOptions("E:\\" , 79607885824) #must be the same as c:
        self.__migrator = Migrator.Migrator(options)

        logging.basicConfig(format='%(asctime)s %(message)s' , filename='migrator.log',level=logging.DEBUG)
        

    def test_migrator(self):
        logging.info("Migrator test started")
        self.__migrator.runFullScenario()
        logging.info("Migrator test ended")

if __name__ == '__main__':
    unittest.main()
