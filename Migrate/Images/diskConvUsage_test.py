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
    
class diskConvUseageTest(unittest.TestCase):
    """ Unittest class for testing usage scenario's: lots of random sized, random offset writes and reads """
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
            
    def testLotsOfRandomLarge(self):
        """ Tests usage scenario with (relatively) large chunks of data written. Tests if random reads produce correct data 
            Will require insertions of null grains
        """
        offset  = 0
        self.removeFile()
        image = StreamVmdkMedia(streamImage)
        image.open()
        rawData = ""
        random.seed(time.time())
        #write RANDOM_COUNT chunks of random sized data at random offset from each other
        #Also stores the written data in a raw data string to compare to later reads
        for i in range(RANDOM_COUNT):
            dataToWrite = ""
            gap = random.randrange(0,GRAIN_SIZE*8)
            dataLength = random.randrange(0,GRAIN_SIZE*8)
            dataLength = divro(dataLength, SECTOR_SIZE) * SECTOR_SIZE
            for j in range(gap):
                rawData += '\0'
            for j in range(dataLength):
                newByte = struct.pack("=B", random.randrange(0,256))
                rawData += newByte
                dataToWrite += newByte
            offset += gap

            image.writeDiskData(offset, dataToWrite)
            offset += dataLength
        
        #assert disk size is as expected
        self.assertEqual(divro(len(rawData),GRAIN_SIZE), divro(image.getMaxSize(),GRAIN_SIZE))
        
        #read all data from the disk
        readData = image.readDiskData(0, len(rawData))
    
        #assert the whole read chunk (minus some padding because we did not read SECTOR_SIZE multiple) is equal to raw data
        self.assertEqual(readData[:len(rawData)], rawData)
        #assert all padding is null
        self.assertTrue(all(v == '\0' for v in readData[len(rawData):]))
        image.reopen()
        #just do the rest again after reopening the image: should be fine
        readData = image.readDiskData(0, len(rawData))
        self.assertEqual(readData[:len(rawData)], rawData)
        self.assertTrue(all(v == '\0' for v in readData[len(rawData):]))
        
        #pad the rawdata the same way that the disk data is padded, for easier checks
        while len(rawData)%SECTOR_SIZE != 0:
            rawData += '\0'
        
        self.assertEqual(rawData, readData)
        image.close()
        #create completely new image object, to make sure that no left over pieces of information from the original image
        #are masking bugs
        image2 = StreamVmdkMedia(streamImage)
        image2.open()
        #read RANDOM_COUNT random chunks of data from the disk and assert they are equal to
        #the same chunks in the raw data string
        for i in range(RANDOM_COUNT):
            offset = random.randrange(0, len(rawData))
            size = random.randrange(0, len(rawData) - offset)
            readData = image2.readDiskData(offset, size)
            self.assertEqual(readData[:size], rawData[offset:offset+size])
        image2.close()
    
    def testLotsOfRandomSmall(self):
        """ Tests usage scenario with (relatively) large chunks of data written. Tests if random reads produce correct data 
            Will require lots of continuing from grain which has already been written to.
            """
        offset  = 0
        self.removeFile()
    
        image = StreamVmdkMedia(streamImage)
        image.open()
        rawData = ""
        random.seed(time.time())
        for i in range(RANDOM_COUNT):
            dataToWrite = ""
            gap = random.randrange(0,SECTOR_SIZE*5)
            dataLength = random.randrange(0,SECTOR_SIZE*5)
            dataLength = divro(dataLength, SECTOR_SIZE) * SECTOR_SIZE
            for j in range(gap):
                rawData += '\0'
            for j in range(dataLength):
                newByte = struct.pack("=B", random.randrange(0,256))
                rawData += newByte
                dataToWrite += newByte
            offset += gap
            image.writeDiskData(offset, dataToWrite)
            offset += dataLength
        
        self.assertEqual(divro(len(rawData),GRAIN_SIZE), divro(image.getMaxSize(),GRAIN_SIZE))
    
        readData = image.readDiskData(0, len(rawData))
    
        self.assertEqual(readData[:len(rawData)], rawData)
        self.assertTrue(all(v == '\0' for v in readData[len(rawData):]))
        image.reopen()
        readData = image.readDiskData(0, len(rawData))
        self.assertEqual(readData[:len(rawData)], rawData)
        self.assertTrue(all(v == '\0' for v in readData[len(rawData):]))
    
        while len(rawData)%SECTOR_SIZE != 0:
            rawData += '\0'
    
        self.assertEqual(rawData, readData)
        image.close()
        #create completely new image object, to make sure that no left over pieces of information from the original image
        #are masking bugs
        image2 = StreamVmdkMedia(streamImage)
        image2.open()
        for i in range(RANDOM_COUNT):
            offset = random.randrange(0, len(rawData))
            size = random.randrange(0, len(rawData) - offset)
            readData = image2.readDiskData(offset, size)
            self.assertEqual(readData[:size], rawData[offset:offset+size])
        image2.close()
