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
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        now = time.localtime()
        
        
    def test_mediapieces(self):
        chunksize = 1024
        overallsize = 1024*1024
        media = GzipChunkMedia.GzipChunkMedia("E:\\rawtest\\arc.tar", overallsize , chunksize, 1) # set no compression to boost operations
        file = open('C:\\procmon.exe', "rb")
        filedata = file.read()
        file.close()

        offset = 16
        datasize = 64

        while datasize < chunksize * 4:
            try:
                offset = 0
                datasize = datasize + 31
                while offset + datasize < overallsize:
                    media.writeDiskData(offset , filedata[0:datasize])
                    data = media.readDiskData(offset , datasize)
                    self.assertEqual(datasize , len(data))
                    self.assertEqual(filedata[0:datasize] , data)
                    offset = offset + 32
            except Exception as e:
                print("Error while testng rw data at offset = " + str(offset) + " size = " + str(datasize))
                raise

        self.assertLessEqual(media.getImageSize() , overallsize)

    def test_z_reuse_arch(self):
        chunksize = 1024
        overallsize = 1024*1024
        media = GzipChunkMedia.GzipChunkMedia("E:\\rawtest\\arc.tar", overallsize , chunksize)
        file = open('C:\\procmon.exe', "rb")
        filedata = file.read()
        file.close()

        

        self.assertEqual(media.getImageSize() , overallsize)
        
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
        self.assertTrue(media.getImageSize() <= overallsize)
        #TODO: make some lively backup from the smae snapshot


    def test_localshare(self):
        chunksize = 1024
        overallsize = 1024*1024
        media = GzipChunkMedia.GzipChunkMedia("\\\\127.0.0.1\\downloads\\rawtest\\tar", overallsize , chunksize)
        file = open('C:\\layout.ini', "r")
        filedata = file.read()
        file.close()

        media.writeDiskData(0 , filedata)
        data = media.readDiskData(0 , len(filedata))
        self.assertEqual(filedata , data)
        self.assertTrue(media.getImageSize() <= overallsize)
        #TODO: make some lively backup from the smae snapshot
      
    def test_nonlocalshare(self):
        logging.info("Test disabled")
        return 
        chunksize = 1024
        overallsize = 1024*1024
        media = GzipChunkMedia.GzipChunkMedia("\\\\FEOFFNOTE\\Downloads\\rawtest\\tar", overallsize , chunksize)
        file = open('C:\\layout.ini', "r")
        filedata = file.read()
        file.close()

        media.writeDiskData(0 , filedata)
        data = media.readDiskData(0 , len(filedata))
        self.assertEqual(filedata , data)
        self.assertTrue(media.getImageSize() <= overallsize)
        #TODO: make some lively backup from the smae snapshot

    def test_frommediatodisk(self):
        return
        
        

if __name__ == '__main__':
    unittest.main()