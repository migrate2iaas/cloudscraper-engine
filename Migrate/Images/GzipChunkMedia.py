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
    """Media consisting of lots of already gzipped chunks"""

    def __init__(self , filename , imagesizeBytes , chunksize , compression=4):
        self.__filename = filename
        self.__diskSize = imagesizeBytes
        self.__chunkSize = chunksize
        self.__overallSize = 0
        self.__compression = compression
        #contains the sizes of written files
        self.__filesWritten = dict()
        super(GzipChunkMedia,self).__init__() 

    #starts the connection
    def open(self):
        return

    def getMaxSize(self):
        return self.__diskSize

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
    # Note: we work as we were one big raw file
    def readImageData(self , offset , size):
        return self.readDiskData(offset, size)

    def getImageChunk(self , chunknumber):
        """
            gets raw (gzipped) data from file
        """
        chunkfilename = self.__filename+"offset"+str(chunknumber*self.__chunkSize)+".gz"
        file = open(chunkfilename, "rb")
        chunk = file.read()
        file.close()
        return chunk

    def getUnzippedChunk(self , chunknumber, skip_non_existing = False):
        """
            gets the next image logical chunk
            Args:
            chunknumber : int - a chunk index to get
            skip_non_existing: Boolean - skipe chunk if it doesn't exists and return nulls instead of it
        """

        if chunknumber*self.__chunkSize >= self.__diskSize:
            logging.warning("!Trying to access disk location above the disk size limits. Offset = " + str(chunknumber*self.__chunkSize) + " , disk size = " + str(self.__diskSize))
            return None

        chunkfilename = self.__filename+"offset"+str(chunknumber*self.__chunkSize)+".gz"

        
        if (os.path.exists(chunkfilename) == False):
            #fails in case there were kinda error in here.
            #e.g. system crashed when this file was just created
            logging.debug(chunkfilename+" created \n")
            file = open(chunkfilename, "wb")
            nullbytes = bytearray(self.__chunkSize)
            if skip_non_existing:
                return str(nullbytes)
            gzipfile = gzip.GzipFile("offset"+str(chunknumber*self.__chunkSize), "wb" , self.__compression , file)
            gzipfile.write(str(nullbytes))
            gzipfile.close()
            file.close()

        file = open(chunkfilename, "rb")
        gzipfile = gzip.GzipFile("offset"+str(chunknumber*self.__chunkSize), "r" , self.__compression , file)
        chunk = gzipfile.read()
        gzipfile.close()
        file.close()

        #in case the file is broken, no gzip data is in it
        if len(chunk) == 0:
            #logging.warning("!Warning: Found bad part in archive " + chunkfilename + " , replacing it");
            os.remove(chunkfilename)
            return self.getUnzippedChunk(chunknumber)

        return chunk
    
    #writes data to the container (as it was a disk)
    def writeDiskData(self, offset, data):
        iniitaloffset = offset
        initialdatalen = len(data)

        chunkoffset = offset % self.__chunkSize
        firstchunk = int(offset / self.__chunkSize)
        size = len(data)
        lastchunk = int((offset + size) / self.__chunkSize)
        lastchunkoffset = (offset + size) % self.__chunkSize
        
        if chunkoffset:
            #here we should read a chunk then write a chunk
            firstdata = self.getUnzippedChunk(firstchunk)
            data = firstdata[0:chunkoffset] + data
            offset = firstchunk*self.__chunkSize
        if lastchunkoffset:
            data = data + self.getUnzippedChunk(lastchunk)[lastchunkoffset:]

        offsetindata = 0
        datasize = len(data)
        while offsetindata < datasize:
            chunkfilename = self.__filename+"offset"+str(offset+offsetindata)+".gz"
            #if self.__filesWritten.has_key(chunkfilename):
            #    self.__overallSize = self.__overallSize - self.__filesWritten[chunkfilename]

            file = open(chunkfilename, "wb")
            gzipfile = gzip.GzipFile("offset"+str(offset+offsetindata), "wb" , self.__compression , file)
            gzipfile.write(data[offsetindata:offsetindata+self.__chunkSize])
            gzipfile.flush()
            gzipfile.close()
            file.flush()
            file.close()
            offsetindata = offsetindata + self.__chunkSize
            # so we should remember somehow was it written at all
            writtenfilesize = os.stat(chunkfilename).st_size
            self.__filesWritten[chunkfilename] = writtenfilesize
        
        self.__overallSize = max(self.__overallSize , initialdatalen+iniitaloffset)
        if self.__overallSize > self.__diskSize:
            logging.warning("!Try to write in gzipped chunks more than allocated")
            logging.debug("Preallocated size is " + str(self.__diskSize) + " . Size of data written is " + str(self.__overallSize) ) 


    #reads data from the container (as it was a disk)
    def readDiskData(self , offset , size):
        # could be done by founding the offset file, gunzip it and read data from it
        if offset+size >= self.__diskSize:
            size = self.__diskSize - offset

        chunkoffset = offset % self.__chunkSize
        firstchunk = int(offset / self.__chunkSize)
        lastchunkoffset = (offset + size) % self.__chunkSize
        lastchunk = int( (offset + size) / self.__chunkSize)
        offset = firstchunk*self.__chunkSize

        currentchunk = firstchunk
        data = str()
        while currentchunk <= lastchunk:
            chunk = self.getUnzippedChunk(currentchunk , True)
            if chunk and len(chunk) > 0:
                data = data + chunk
            else:
                break
            currentchunk = currentchunk + 1
        return data[chunkoffset:(lastchunk-firstchunk)*self.__chunkSize+lastchunkoffset]

    #gets the overall image size available for reading. Note: it is subject to grow when new data is written
    def getImageSize(self):
        self.flush()
        # self.__overallSize could be null in case of image created by previous transfer
        #if self.__overallSize > 0:
        #    return self.__overallSize
        # overallSize is buggy for now
        return self.__diskSize

    #sets the channel so the data may be sent simultaniously. Not implemented for now
    def setChannel(self):
        raise NotImplementedError
