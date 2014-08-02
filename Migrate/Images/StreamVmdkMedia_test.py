from StreamVmdkMedia import StreamVmdkMedia
import unittest
import os
import time
import random
import struct
import logging
import zlib
import math

SECTOR_SIZE = 512
GRAIN_SIZE = 512*128
streamImage = "StreamVmdkImageTest.vmdk"
rawImage = "StreamVmdkRawImageTest"
RANDOM_COUNT = 100

#round up divide
def divro(num, den):
    return int(math.ceil((1.0*num)/(1.0*den)))
    
class diskConvTest(unittest.TestCase):
    
    def removeFile(self):
        try:
            os.remove(streamImage)
        except:
            try:
                os.remove(rawImage)
                return
            except:
                return
        try:
            os.remove(rawImage)
            return
        except:
            return 
         
        
    
    def testReadNew(self):
        """ Assert the image is initialized as expected """
        self.removeFile()
        image = StreamVmdkMedia(streamImage)
        image.open()
        self.assertEqual(image.getMaxSize(), 0)
        self.assertEqual(image.readDiskData(0,image.getMaxSize()), "")
        with self.assertRaises(Exception):
            image.readDiskData(1,1)
        image.close()
    
    def testReadOnly(self):
        """ Assert an existing image is indeed in read only mode """
        self.removeFile()
        image = StreamVmdkMedia(streamImage)
        image.open()
        image.close()
        image.open()
        with self.assertRaises(Exception):
            image.writeDiskData(0,"1")
        image.close()
        
    def testReadFromSessionWrite(self):
        """ Assert the reading from a newly created image that has not yet been closed goes as expected """
        self.removeFile()
        image = StreamVmdkMedia(streamImage)
        image.open()
        self.assertEqual(image.getMaxSize(), 0)
        image.writeDiskData(0,"test")
        self.assertEqual(image.getMaxSize(), GRAIN_SIZE)
        data = image.readDiskData(0,SECTOR_SIZE)
        self.assertEqual(len(data),SECTOR_SIZE)
        self.assertEqual(data[:4],"test")
        self.assertTrue(all(v == '\0' for v in data[4:SECTOR_SIZE]))#Everything after the 'test' should be \0 initialized
        with self.assertRaises(Exception):
            image.readDiskData(GRAIN_SIZE + 1, 1)
        
        data = image.readDiskData(0,image.getMaxSize())
        self.assertEqual(len(data), GRAIN_SIZE)#The disk should now be exactly 1 grain long
        self.assertEqual(data[:4],"test")
        self.assertTrue(all(v == '\0' for v in data[4:GRAIN_SIZE]))
        
        
        image.writeDiskData(GRAIN_SIZE*2, "test2")
        data = image.readDiskData(0,image.getMaxSize())
        self.assertEqual(len(data), 3*GRAIN_SIZE)
        self.assertEqual(data[:4],"test")
        self.assertEqual(data[GRAIN_SIZE*2:GRAIN_SIZE*2+5], "test2")
        self.assertTrue(all(v == '\0' for v in data[4:GRAIN_SIZE*2]))#Everything between the two tests should be \0
        self.assertTrue(all(v == '\0' for v in data[GRAIN_SIZE*2+5:GRAIN_SIZE*3]))#The empty area after the second test should be \0
        image.close()
        
    def testReadFromSessionWritePreSetSize(self):
        """ Assert the reading from a newly created image that has not yet been closed goes as expected
            This image's size is set on creation """ 
        self.removeFile()
        image = StreamVmdkMedia(streamImage, 3*GRAIN_SIZE)
        image.open()
        self.assertEqual(image.getMaxSize(), 3*GRAIN_SIZE)
        
        data = image.readDiskData(0, 3*GRAIN_SIZE)
        self.assertTrue(all(v == '\0' for v in data))
        
        image.writeDiskData(0, "test")
        self.assertEqual(image.getMaxSize(), 3*GRAIN_SIZE)
        
        data = image.readDiskData(0, 3*GRAIN_SIZE)
        self.assertTrue(data[:4] == "test")
        self.assertTrue(all(v == '\0' for v in data[4:]))
        
        with self.assertRaises(Exception):
            image.readDiskData(GRAIN_SIZE*3+1, 10)
        with self.assertRaises(Exception):
            image.writeDiskData(GRAIN_SIZE*3+1, "test")
        
        offset = int(GRAIN_SIZE*1.5)
        image.writeDiskData(offset, "test")
        data = image.readDiskData(0, GRAIN_SIZE*3)
        self.assertTrue(data[:4] == "test")
        self.assertTrue(all(v == '\0' for v in data[4:offset]))
        self.assertTrue(data[offset:offset+4] == "test")
        self.assertTrue(all(v == '\0' for v in data[offset+4:]))
        
        image.close()
        
    def testFileSize(self):
        """ Test file size behaves as expected """
        self.removeFile()
        image = StreamVmdkMedia(streamImage)
        image.open()
        
        zeroGrain = ""
        for i in range(GRAIN_SIZE):
            zeroGrain += '\0'
        
        image.writeDiskData(0, zeroGrain)
        image.writeDiskData(GRAIN_SIZE,zeroGrain)
        image.writeDiskData(2*GRAIN_SIZE, zeroGrain)
        
        image.close()
        self.assertEqual(os.path.getsize(streamImage), SECTOR_SIZE*9)#header, sector, sector, sector, GD marker, GD, footer mark, footer, EOS marker = 9 sectors

    def testReadEmpty(self):
        """ Assert disk size behaves as expected """
        self.removeFile()
        
        image = StreamVmdkMedia(streamImage)
        image.open()
        
        zeroGrain = ""
        for i in range(GRAIN_SIZE):
            zeroGrain += '\0'
        
        image.writeDiskData(0, zeroGrain)
        image.writeDiskData(GRAIN_SIZE,zeroGrain)
        image.writeDiskData(2*GRAIN_SIZE, zeroGrain)   
        
        image.reopen()
        
        data = image.readDiskData(0, image.getMaxSize())
        self.assertEqual(len(data), 3*GRAIN_SIZE)
        self.assertTrue(all(v == '\0' for v in data))
        image.close()
        
        self.removeFile()
          
        image = StreamVmdkMedia(streamImage, GRAIN_SIZE*3)
        image.open()
        image.reopen()
        
        data = image.readDiskData(0, image.getMaxSize())
        self.assertEqual(len(data), 3*GRAIN_SIZE)
        self.assertTrue(all(v == '\0' for v in data))
        image.close()

    def testConvert(self):
        """ invokes VBoxManage to convert a created image to a raw format, and test if it produces the correct output """
        try:
            os.remove(rawImage + "1")
        except:
            pass
        random.seed(time.time())
        
        data = ""
        for i in range(GRAIN_SIZE*5):
            data += struct.pack("B",random.randrange(0,256))
            
        
        
        self.removeFile()
        
        image = StreamVmdkMedia(streamImage, GRAIN_SIZE*5)
        image.open()
        
        image.writeDiskData(0, data)
        image.close()
        
        os.system( "VBoxManage clonehd --format RAW " + streamImage + " " + rawImage )
        
        readData = open(rawImage, "rb").read()
        
        if len(readData) > GRAIN_SIZE*5:
            logging.warning("known bug: VBoxManage outputs too big images. raw image size: %s, expected size: %s, grains too much: %s"%(len(readData), GRAIN_SIZE*5, (len(readData)-GRAIN_SIZE*5)/(128*512)))
        
        self.assertEqual(data, readData[:GRAIN_SIZE*5])
        
        self.removeFile()
        image = StreamVmdkMedia(streamImage)
        image.open()
        
        image.writeDiskData(0, data)
        image.close()
        os.system( "VBoxManage clonehd --format RAW " + streamImage + " " + rawImage+"1")#must change the file name because Vbox will start complaining about UUIDs otherwise.
        
        readData = open(rawImage+"1", "rb").read()
        
        if len(readData) > GRAIN_SIZE*5:
            logging.warning("known bug: for small virtual disks VBoxManage outputs too big raw files. raw image size: %s, expected size: %s, grains too much: %s"%(len(readData), GRAIN_SIZE*5, (len(readData)-GRAIN_SIZE*5)/(128*512)))
        
        self.assertEqual(data, readData[:GRAIN_SIZE*5])
        
    def testReadImage(self):
        """ Assert reading from the image file produces expected results """
        self.removeFile()
        
        image = StreamVmdkMedia(streamImage, GRAIN_SIZE*5)
        image.open()
        with self.assertRaises(Exception):
            image.readImageData(0,SECTOR_SIZE*5)
        data = image.readImageData(0,SECTOR_SIZE*4)
        self.assertTrue(all(v == '\0' for v in data))
        
        #construct well-compressing piece of data filling up one grain. must be non empty, because otherwise it would not get written
        data = "test"
        while len(data) != GRAIN_SIZE:
            data += '\0'

        image.writeDiskData(0, data)
        self.assertEqual(SECTOR_SIZE*5, image.getImageSize())
        ReadData = image.readImageData(SECTOR_SIZE*4, SECTOR_SIZE)
        self.assertTrue(all(v == '\0' for v in ReadData[:8]))#sector offset on disk = 0
        
        self.assertEqual(struct.unpack("=I",ReadData[8:12])[0], len(zlib.compress(data)))
        self.assertEqual(ReadData[12:12 + len(zlib.compress(data))], zlib.compress(data))
        self.assertTrue(all(v == '\0' for v in ReadData[12 + len(zlib.compress(data)):]))
        
        image.close()
         
     
    def test_correctFilePos(self):
        self.removeFile()
        image = StreamVmdkMedia(streamImage)
        image.open()
        image.writeDiskData(0,"test")
        image.writeDiskData(GRAIN_SIZE, "test")
        image.readDiskData(0,SECTOR_SIZE)
        image.writeDiskData(GRAIN_SIZE*2,"test")
        image.close()
        self.assertEqual(GRAIN_SIZE*3, image.getMaxSize())
        data = image.readDiskData(0, GRAIN_SIZE*3)
        self.assertTrue(all(v == '\0' for v in data[4:GRAIN_SIZE]) )        
        self.assertTrue(all(v == '\0' for v in data[GRAIN_SIZE + 4:2*GRAIN_SIZE]) )   
        self.assertTrue(all(v == '\0' for v in data[GRAIN_SIZE*2+4:3*GRAIN_SIZE]) )
        self.assertEqual(data[0:4], "test")
        self.assertEqual(data[GRAIN_SIZE:GRAIN_SIZE+4], "test")
        self.assertEqual(data[GRAIN_SIZE*2:GRAIN_SIZE*2 + 4], "test")
    
    def test_operationsAfterClose(self):
        self.removeFile()
        image = StreamVmdkMedia(streamImage)
        image.open()
        image.writeDiskData(0,"test")
        image.close()
        try:
            self.assertEqual(image.getMaxSize(), GRAIN_SIZE)#might as well do some assertions while we are testing for exceptions
            self.assertEqual(image.getImageSize(), 15*SECTOR_SIZE)
            self.assertEqual(image.readDiskData(0,GRAIN_SIZE)[:4], "test")
            self.assertEqual(struct.unpack("=Q",image.readImageData(0,SECTOR_SIZE)[12:20])[0],128) 
        except:
            self.fail("exception raised on permitted opeteration after close() is called")
            
        

         
 