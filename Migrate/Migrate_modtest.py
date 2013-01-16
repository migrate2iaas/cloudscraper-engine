import sys
import os

sys.path.append('.\Windows')

import WindowsVolume
import WindowsBackupSource
import AdjustedBackupSource
import BackupAdjust
import WindowsVolumeTransferTarget
import VssThruVshadow

import WindowsVolume
import WindowsBackupSource
import AdjustedBackupSource
import BackupAdjust

import unittest

#todo: move it to kinda test module
class Migrate_test(unittest.TestCase):

    def test_migrate(self):
        windir = os.environ['Windir'];
        windir = windir.lower()
        windrive_letter = windir[0];
        system_vol = "\\\\.\\"+windrive_letter+":"

        print("Copy started");
        print("Creating snapshot")
        Vss = VssThruVshadow.VssThruVshadow()
        snapname = Vss.createSnapshot(system_vol)
        snapvol = snapname

        print("Adjusting target and source volumes");

        WinVol = WindowsVolume.WindowsVolume(snapname)
        WinBackupSource = WindowsBackupSource.WindowsBackupSource()
        WinBackupSource.setBackupDataSource(WinVol)

        adjustedBackupSource = AdjustedBackupSource.AdjustedBackupSource()
        adjustedBackupSource.setBackupSource(WinBackupSource)


        adjust = BackupAdjust.BackupAdjust()
        #TODO: need more flexibility in filenames. Source files without letters are needed
        #adjust.removeFile(snapvol+"\\windows"+"\\system32\\config\\system")
        #adjust.removeFile(snapvol+"\\windows"+"\\system32\\config\\software")
        adjust.removeFile(snapvol+"\\pagefile.sys")

        adjustedBackupSource.setAdjustOption(adjust)
        extents = adjustedBackupSource.getFilesBlockRange()

        overallsize = 0
        for extent in extents:
            overallsize = overallsize + extent.getSize()
            extent.setData(WindowsVolume.DeferedReader(extent, WinVol))
        print ("Overall data size after block removal: " + str(overallsize))

        WinTargetVol = WindowsVolumeTransferTarget.WindowsVolumeTransferTarget("\\\\.\\X:")
        WinTargetVol.TransferRawData(extents)

        #add file compare to the test

        Vss.deleteSnapshot(snapname)

# The test to be executed on virtual machine
if __name__ == '__main__':
    unittest.main()




#the draft of product code:

#0. check the system

#1. create the container



#2. set up source and dest

#3. get some data from source, adjust it locally

#4. backup with adjusted files

#5. init transmition (could be in parallel with backup)