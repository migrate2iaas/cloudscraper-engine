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
import logging

class AdjusteBackupSource_test(unittest.TestCase):

    #TODO: make more sophisticated config\test reading data from some config. dunno

    def setUp(self):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)

        self.__WinVol = WindowsVolume.WindowsVolume("\\\\.\\D:")
        self.__WinBackupSource = WindowsBackupSource.WindowsBackupSource()
        self.__WinBackupSource.setBackupDataSource(self.__WinVol)
        self.__AdjustedBackupSource = AdjustedBackupSource.AdjustedBackupSource()
        self.__AdjustedBackupSource.setBackupSource(self.__WinBackupSource)




    def test_noadjust(self):
        adjust = BackupAdjust.BackupAdjust()
        self.__AdjustedBackupSource.setAdjustOption(adjust)
        numfiles = 0
        for files in self.__AdjustedBackupSource.getFileEnum():
            numfiles = numfiles + 1
        print ('File Count = ' +  str(numfiles))
        self.assertTrue(numfiles > 0)
       
    def test_remove(self):
        adjust = BackupAdjust.BackupAdjust()
        adjust.removeFile("\\\\.\\d:\\bootmgr")
        adjust.removeFile("\\\\.\\d:\\bootsect.bak")
        self.__AdjustedBackupSource.setAdjustOption(adjust)
        for files in self.__AdjustedBackupSource.getFileEnum():
            self.assertTrue(files.getName() != "\\\\.\\d:\\bootmgr")
            self.assertTrue(files.getSourcePath() != "\\\\.\\d:\\bootmgr")
            self.assertFalse( "bootsect.bak" in files.getName())

    def test_add(self):
        adjust = BackupAdjust.BackupAdjust()
        adjust.addFile("c:\\windows\\write.exe" , "write.exe");

        self.__AdjustedBackupSource.setAdjustOption(adjust)
        found = False
        for files in self.__AdjustedBackupSource.getFileEnum():
            if ("write.exe" in files.getName()):
                found = True
        self.assertTrue(found)
 
    def test_blockquery(self):
        adjust = BackupAdjust.BackupAdjust()
        self.__AdjustedBackupSource.setAdjustOption(adjust)
        overallsize = 0
        for block in self.__AdjustedBackupSource.getFilesBlockRange():
            overallsize = overallsize + block.getSize()
        print ("Overall data size: " + str(overallsize))
    

    def test_blockremove(self):
        adjust = BackupAdjust.BackupAdjust()
        adjust.removeFile("\\\\.\\d:\\bootmgr")
        adjust.removeFile("\\\\.\\d:\\bootsect.bak")
        self.__AdjustedBackupSource.setAdjustOption(adjust)
        overallsize = 0
        for block in self.__AdjustedBackupSource.getFilesBlockRange():
            overallsize = overallsize + block.getSize()
        print ("Overall data size after block removal: " + str(overallsize))
        
    def test_blockreplacefile(self):
        adjust = BackupAdjust.BackupAdjust()
        #adjust.replaceFile("log.txt", "\\\\.\\c:\\log.txt")
        self.__AdjustedBackupSource.setAdjustOption(adjust)
        blocks = self.__AdjustedBackupSource.getFileBlockRange("log.txt")
        data = blocks[0].getData()
        self.assertTrue("F\x00R\x00O\x00M\x00D\x00I\x00S\x00K" in data);

        adjust = BackupAdjust.BackupAdjust()
        adjust.replaceFile("log.txt", "\\\\.\\c:\\log.txt")
        self.__AdjustedBackupSource.setAdjustOption(adjust)
        blocks = self.__AdjustedBackupSource.getFileBlockRange("log.txt")
        data = blocks[0].getData()
        self.assertFalse("F\x00R\x00O\x00M\x00D\x00I\x00S\x00K" in data);
        #print (blocks[0].getData())
        
    def test_blockreplacefiles(self):
        adjust = BackupAdjust.BackupAdjust()
        self.__AdjustedBackupSource.setAdjustOption(adjust)
        fileblocks = self.__AdjustedBackupSource.getFileBlockRange("log.txt")

        adjust.replaceFile("log.txt", "\\\\.\\c:\\log.txt")
        self.__AdjustedBackupSource.setAdjustOption(adjust)
        
        blockfound = False
        for block in self.__AdjustedBackupSource.getFilesBlockRange():
            if fileblocks[0] in block:
                self.assertFalse("F\x00R\x00O\x00M\x00D\x00I\x00S\x00K" in block.getData());
                self.assertTrue("F\x00R\x00O\x00M\x00D\x00I\x00S\x00K" in fileblocks[0].getData());
                blockfound = True
        self.assertTrue(blockfound)

if __name__ == '__main__':
    unittest.main()




