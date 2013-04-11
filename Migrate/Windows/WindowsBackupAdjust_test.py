import sys


sys.path.append('.\Windows')

import unittest
import WindowsVolume
import WindowsBackupSource
import AdjustedBackupSource
import BackupAdjust
import WindowsVolumeTransferTarget
import WindowsBackupAdjust
import SystemAdjustOptions
import logging

class AdjustOptionTest(SystemAdjustOptions.SystemAdjustOptions):
    
    diskScsi = 1
    diskAta = 2

    def __init__(self):
        super(AdjustOptionTest,self).__init__()

    def getPregeneratedBcdHivePath(self):
        return "..\\..\\resources\\boot\\win\\BCD_MBR"

    def getNewMbrId(self):
        return 0x02F47846
        
    def getNewSysPartStart(self):
        return 0x0100000

    def getSysDiskType(self):
        return self.diskScsi

    def rdpPort(self):
        return 15500

    def fixRDP(self):
        return True

class WindowsBackupAdjust_test(unittest.TestCase):
     #TODO: make more sophisticated config\test reading data from some config. dunno
    """description of class"""

    def setUp(self):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        self.__WinTargetVol = WindowsVolumeTransferTarget.WindowsVolumeTransferTarget("\\\\.\\X:")
        self.__WinVol = WindowsVolume.WindowsVolume("\\\\.\\D:")
        self.__WinBackupSource = WindowsBackupSource.WindowsBackupSource()
        self.__WinBackupSource.setBackupDataSource(self.__WinVol)
        self.__AdjustedBackupSource = AdjustedBackupSource.AdjustedBackupSource()
        self.__AdjustedBackupSource.setBackupSource(self.__WinBackupSource)
       
        
        

    def test_rawcopy(self):
        adjust = WindowsBackupAdjust.WindowsBackupAdjust(AdjustOptionTest())
        adjust.configureBackupAdjust(self.__WinBackupSource)
        self.__AdjustedBackupSource.setAdjustOption(adjust)
        
        extents = self.__AdjustedBackupSource.getFilesBlockRange()
        self.__WinTargetVol.transferRawData(extents)

if __name__ == '__main__':
    unittest.main()


 


