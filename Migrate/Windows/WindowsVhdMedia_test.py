# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import sys

sys.path.append(sys.path[0]+'\\..')
sys.path.append('.\Windows')

import unittest
import logging
import WindowsVhdMedia
import SimpleDiskParser
import SimpleDataTransferProto
import time
import WindowsDeviceDataTransferProto

import unittest
import WindowsVolume
import WindowsBackupSource
import SystemAdjustOptions
import logging
import DataExtent
import os

class WindowsVhdMedia_test(unittest.TestCase):
     #TODO: make more sophisticated config\test reading data from some config. dunno
    """description of class"""

    def setUp(self):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        now = time.localtime()
        
        
    def test_emptydisk(self):
        imagesize = 512*1024*1024*1024
        imagepath = "E:\\vhdtest2.vhd"
        media = WindowsVhdMedia.WindowsVhdMedia(imagepath , imagesize+1024*1024)
        self.assertTrue(media.open())
        datatransfer = WindowsDeviceDataTransferProto.WindowsDeviceDataTransferProto(media.getWindowsDevicePath(), media.getWindowsDiskNumber() , media)
        parser = SimpleDiskParser.SimpleDiskParser(datatransfer , 0xeda)
        transfertarget = parser.createTransferTarget(imagesize)
        #TODO: test media is ok
        #TODO: need kinda automate it in order to reinit
        media.close()
        os.remove(imagepath)


    def test_utfdisk(self):
        imagesize = 512*1024*1024*1024
        imagepath = u"E:\\\u4500abc.vhd"
        media = WindowsVhdMedia.WindowsVhdMedia(imagepath , imagesize+1024*1024)
        self.assertTrue(media.open())
        datatransfer = WindowsDeviceDataTransferProto.WindowsDeviceDataTransferProto(media.getWindowsDevicePath(), media.getWindowsDiskNumber() , media)
        parser = SimpleDiskParser.SimpleDiskParser(datatransfer , 0xeda)
        transfertarget = parser.createTransferTarget(imagesize)
        #TODO: test media is ok
        #TODO: need kinda automate it in order to reinit
        media.close()
        os.remove(imagepath)

    def test_utfdiskreopen(self):
        imagesize = 512*1024*1024*1024
        imagepath = u"E:\\\u4500abc2.vhd"
        media = WindowsVhdMedia.WindowsVhdMedia(imagepath , imagesize+1024*1024)
        self.assertTrue(media.open())
        datatransfer = WindowsDeviceDataTransferProto.WindowsDeviceDataTransferProto(media.getWindowsDevicePath(), media.getWindowsDiskNumber() , media)
        parser = SimpleDiskParser.SimpleDiskParser(datatransfer , 0xeda)
        transfertarget = parser.createTransferTarget(imagesize)
        #TODO: test media is ok
        #TODO: need kinda automate it in order to reinit
        media.close()
        self.assertTrue(media.open())
        media.close()
        os.remove(imagepath)
    
    
    def test_sharedisk(self):
        #virtual disk doesn't seem to open on a share pointing to local machine due to vhd support limitations
        return
        imagesize = 512*1024*1024*1024
        imagepath = "\\\\127.0.0.1\\downloads\\share.vhd"
        media = WindowsVhdMedia.WindowsVhdMedia(imagepath , imagesize+1024*1024)
        self.assertFalse(media.open())
        

    def test_nonemptydisk(self):
        return
        #NOTE: rewrites are available only if we use WindowsDiskParser. Writes are blocked otherwise
        imagesize = 512*1024*1024*1024
        imagepath = "E:\\vhdtest4.vhd"
        media = WindowsVhdMedia.WindowsVhdMedia(imagepath , imagesize+1024*1024)
        media.open()
        datatransfer = WindowsDeviceDataTransferProto.WindowsDeviceDataTransferProto(media.getWindowsDevicePath(), media.getWindowsDiskNumber() , media)
        parser = SimpleDiskParser.SimpleDiskParser(datatransfer , 0xeda1)
        transfertarget = parser.createTransferTarget(imagesize)

        #createa two volumes on 1 disk. G is first (and starts from the first disk) and H is the second.
        #the test is to transfer filesystem form H to vhdtest.vhd with the bootsector loaded from G
        self.__WinVol2 = WindowsVolume.WindowsVolume("\\\\.\\D:")
        extent = DataExtent.DataExtent(0, 4096)
        boot2 = self.__WinVol2.readExtent(extent)

        file = open("E:\\d_bootsector.raw", "wb")
        file.write(boot2)
        file.close()

        self.__systemBackupSource = WindowsBackupSource.WindowsBackupSource()
        self.__systemBackupSource.setBackupDataSource(self.__WinVol2)
        blocks = self.__systemBackupSource.getFilesBlockRange()
        transfertarget.transferRawData(blocks)
        media.close()
        

if __name__ == '__main__':
    unittest.main()