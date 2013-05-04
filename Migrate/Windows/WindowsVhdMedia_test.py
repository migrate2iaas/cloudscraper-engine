# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import sys

sys.path.append(sys.path[0]+'\\..')

import unittest
import logging
import WindowsVhdMedia
import SimpleDiskParser
import SimpleDataTransferProto
import time
import WindowsDeviceDataTransferProto

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
        media.open()
        datatransfer = WindowsDeviceDataTransferProto.WindowsDeviceDataTransferProto(media.getWindowsDevicePath(), media.getWindowsDiskNumber() , media)
        parser = SimpleDiskParser.SimpleDiskParser(datatransfer , 0xeda)
        transfertarget = parser.createTransferTarget(imagesize)
        #TODO: need kinda automate it in order to reinit
        

if __name__ == '__main__':
    unittest.main()