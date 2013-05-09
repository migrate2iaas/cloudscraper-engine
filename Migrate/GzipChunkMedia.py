# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import threading
import DataExtent
import time
import tempfile
import warnings
import logging

import sys
import os
import MigrateExceptions
import subprocess
import re

import logging
import threading 
import datetime

import zlib
import gzip
import tarfile
import ImageMedia


# the base class for all the media (VHD,raw files etc) to contain a system or data image
class GzipChunkMedia(ImageMedia.ImageMedia):
    """Media consisting of one tar with lots of already gzipped chunks"""

    def __init__(self , filename , imagesizeBytes , chunksize):
        self.__filename = filename
        self.__diskSize = imagesizeBytes
        self.__chunkSize = chunksize
        self.__overallSize = 0
        #contains the sizes of written files
        self.__filesWritten = dict()
        return 

    #starts the connection
    def open(self):
        return

    def getMaxSize(self):
        return self.__disksize

    def reopen(self):
        self.close()
        self.open()

    def close(self):
        # here we make tar from all the files
        return

    def flush(self):
        return
    
    def release(self):
        #TODO: refcounts should be added
        self.close()

    #reads data from image, returns data buffer
    # data chunks of variable size
    # the image is not continous, really but one gz could really be split and merged
    def readImageData(self , offset , size):
        #TODO: implement: the data from different fiels should be merged here. 
        #since gz file is number of gzip frames, it's headerless so we could merge all the files in one big gz file
        return

    # get the next image logical chunk
    # now it returns data only but really it could return 
    # more data including chunk special data
    def getImageChunk(self , chunknumber):
        chunkfilename = self.__filename+"offset"+str(chunknumber*self.__chunkSize)+".gz"
        file = open(chunkfilename, "r")
        chunk = file.read()
        file.close()
        return chunk
    
    #writes data to the container (as it was a disk)
    def writeDiskData(self, offset, data):
        
        chunkoffset = offset % self.__chunkSize
        firstchunk = int(offset / self.__chunkSize)
        size = len(data)
        lastchunkoffset = (offset + size) % self.__chunkSize
        
        if chunkoffset:
            #here we should read a chunk then write a chunk
            data = self.getImageChunk(firstchunk) + data[(firstchunk+1)*self.__chunkSize:]
            offset = firstchunk*self.__chunkSize
        if lastchunkoffset:
            data = data + self.getImageChunk(firstchunk)[lastchunkoffset:]

        offsetindata = 0
        while offsetindata < len(data):
            chunkfilename = self.__filename+"offset"+str(offset+offsetindata)+".gz"
            if self.__filesWritten.has_key(chunkfilename):
                self.__overallSize = self.__overallSize - self.__filesWritten[chunkfilename]

            file = open(chunkfilename, "w")
            gzipfile = gzip.GzipFile("offset"+str(offset+offsetindata), "wb" , 8 , file)
            gzipfile.write(data[offsetindata:offsetindata+self.__chunkSize])
            gzipfile.close()
            file.close()
            offsetindata = offsetindata + self.__chunkSize
            # so we should remember somehow was it written at all
            writtenfilesize = os.stat(chunkfilename).st_size
            self.__filesWritten[chunkfilename] = writtenfilesize
            self.__overallSize = self.__overallSize + writtenfilesize


    #reads data from the container (as it was a disk)
    def readDiskData(self , offset , size):
        #TODO: implement reading
        # could be done by founding the offset file, gunizp it and read data from it
        return 

    #gets the overall image size available for reading. Note: it is subject to grow when new data is written
    def getImageSize(self):
        self.flush()
        return self.__overallSize

    #sets the channel so the data may be sent simultaniously. Not implemented for now
    def setChannel(self):
        raise NotImplementedError
