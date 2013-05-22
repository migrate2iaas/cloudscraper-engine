# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys

import unittest
import logging
import GzipChunkMedia
import time


class GzipChunkMedia_test(unittest.TestCase):
     #TODO: make more sophisticated config\test reading data from some config. dunno
    """description of class"""

    def setUp(self):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        now = time.localtime()
        
        
    def test_mediapieces(self):
        chunksize = 1024
        overallsize = 1024*1024
        media = GzipChunkMedia.GzipChunkMedia("E:\\rawtest.tar", overallsize , chunksize)
        file = open('C:\\layout.ini', "r")
        filedata = file.read()
        file.close()

        offset = 16
        datasize = 64

        while datasize < chunksize * 4:
            offset = 0
            datasize = datasize + 31
            while offset + chunksize < overallsize:
                media.writeDiskData(offset , filedata[0:datasize])
                data = media.readDiskData(offset , datasize)
                self.assertEqual(filedata[0:datasize] , data)
                offset = offset + 32

        
    def test_mediawhole(self):
        chunksize = 1024
        overallsize = 1024*1024
        media = GzipChunkMedia.GzipChunkMedia("E:\\rawtest_file.tar", overallsize , chunksize)
        file = open('C:\\layout.ini', "r")
        filedata = file.read()
        file.close()

        media.writeDiskData(0 , filedata)
        data = media.readDiskData(0 , len(filedata))
        self.assertEqual(filedata , data)
        #TODO: make some lively backup from the smae snapshot

    def test_frommediatodisk(self):

        
        

if __name__ == '__main__':
    unittest.main()