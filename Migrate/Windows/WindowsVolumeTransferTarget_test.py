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
import logging

class WindowsVolumeTransferTarget_test(unittest.TestCase):
     #TODO: make more sophisticated config\test reading data from some config. dunno

    def setUp(self):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        
        self.__WinTargetVol = WindowsVolumeTransferTarget.WindowsVolumeTransferTarget("\\\\.\\X:")
        self.__WinVol = WindowsVolume.WindowsVolume("\\\\.\\D:")
        self.__WinBackupSource = WindowsBackupSource.WindowsBackupSource()
        self.__WinBackupSource.setBackupDataSource(self.__WinVol)
        

    def test_rawcopy(self):
        # make sure the shuffled sequence does not lose any elements
        #self.__WinVol.lock()
        extents = self.__WinBackupSource.getFilesBlockRange()
        for extent in extents:
            extent.setData(WindowsVolume.DeferedReader(extent, self.__WinVol))
        self.__WinTargetVol.transferRawData(extents)

if __name__ == '__main__':
    unittest.main()

