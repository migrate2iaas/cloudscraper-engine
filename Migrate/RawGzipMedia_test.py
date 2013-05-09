# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys

import unittest
import logging
import RawGzipMedia
import SimpleDiskParser
import SimpleDataTransferProto
import time


class RawGzipMedia_test(unittest.TestCase):
     #TODO: make more sophisticated config\test reading data from some config. dunno
    """description of class"""

    def setUp(self):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        now = time.localtime()
        
        
    def test_emptydisk(self):
        imagesize = 128*1024*1024
        imagepath = "E:\\rawtest.raw.gz"
        media = RawGzipMedia.RawGzipMedia(imagepath , imagesize+1024*1024)
        media.open()
        parser = SimpleDiskParser.SimpleDiskParser(SimpleDataTransferProto.SimpleDataTransferProto(media) , 0xeda)
        transfertarget = parser.createTransferTarget(imagesize)
        

if __name__ == '__main__':
    unittest.main()