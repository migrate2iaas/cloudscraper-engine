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
import VssThruVshadow

class VSS_test(unittest.TestCase):
     

    def setUp(self):
        self.__WinTargetVol = WindowsVolumeTransferTarget.WindowsVolumeTransferTarget("\\\\.\\X:")
        self.__Vss = VssThruVshadow.VssThruVshadow()
     
    def test_create(self):
 
        print("Testing VSS snapshots, it may take some time (approx 2 mins)")
        snap1 = self.__Vss.createSnapshot("D:");
        snap2 = self.__Vss.createSnapshot("C:");

        self.__Vss.deleteSnapshot(snap2);
        self.__Vss.deleteSnapshot(snap1);

        exceptedDeleteSame = False
        try:
            self.__Vss.deleteSnapshot(snap1);
        except:
            exceptedDeleteSame = True
        self.assertTrue(exceptedDeleteSame)

        exceptedDeleteRandom = False
        try:
            self.__Vss.deleteSnapshot("sdasada");
        except:
            exceptedDeleteRandom = True
        self.assertTrue(exceptedDeleteRandom)

        
        exceptedCreateBad = False
        try:
            self.__Vss.createSnapshot("4:");
        except:
            exceptedCreateBad = True
        self.assertTrue(exceptedCreateBad)

    def test_rawcopy(self):
        print("Testing copy, it may take some time (approx 2 mins)")
        snapname = self.__Vss.createSnapshot("\\\\.\\D:")
        snapvol = snapname
        print("SnapVol to use " + snapvol)
        self.__WinVol = WindowsVolume.WindowsVolume(snapvol)
        self.__WinBackupSource = WindowsBackupSource.WindowsBackupSource()
        self.__WinBackupSource.setBackupDataSource(self.__WinVol)
        
        extents = self.__WinBackupSource.getFilesBlockRange()
        for extent in extents:
            extent.setData(WindowsVolume.DeferedReader(extent, self.__WinVol))
        self.__WinTargetVol.transferRawData(extents)

        self.__Vss.deleteSnapshot(snapname)

if __name__ == '__main__':
    unittest.main()
