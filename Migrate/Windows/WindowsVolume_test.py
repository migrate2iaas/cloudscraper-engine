# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('.\Windows')

import WindowsVolume
import unittest
import logging

class WindowsVolume_test(unittest.TestCase):

    #TODO: make more sophisticated config\test reading data from some config. dunno

    def setUp(self):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        self.WinVol = WindowsVolume.WindowsVolume("\\\\.\\D:")
       

    def test_init(self):
        # make sure the shuffled sequence does not lose any elements
        
        self.assertTrue(self.WinVol.getFileSystem() != None)
        self.assertEqual(self.WinVol.getFileSystem(), "NTFS")

    def test_bitmap(self):
        self.assertIsNotNone(self.WinVol.getFilledVolumeBitmap())

    def test_filelist(self):
        enumerator = self.WinVol.getFileEnumerator()
        for enum in enumerator:
            print(enum)

    def test_filelistmasked(self):
        enumerator = self.WinVol.getFileEnumerator("\\boot\\")
        for enum in enumerator:
            print(enum)
       
    def test_size(self):
        print (self.WinVol.getVolumeSize())

    def test_blocks(self):
        extents = self.WinVol.getFileDataBlocks("bootmgr")
        self.assertIsNotNone(extents)
        print(len(extents))
        print (extents[0])
        self.assertTrue(len(extents)>0)

    def test_read(self):
        extents = self.WinVol.getFileDataBlocks("bootmgr")
        self.assertIsNotNone(extents)
        self.assertTrue(len(extents)>0)
        print ("Read bytes " + str(len(self.WinVol.readExtent(extents[0]) )) )
    

if __name__ == '__main__':
    unittest.main()



