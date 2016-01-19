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
import ImageMedia


# the base class for all the media (VHD,raw files etc) to contain a system or data image
class RawGzipMedia(ImageMedia.ImageMedia):
    """Base class to represent media based on gzipped raw file"""

    # zipLevel could be from 0 to 9. If 0 then no zip occursm if zipLevel is more than 1 the writes should be sequental
    def __init__(self , filename , imagesizeBytes, zipLevel = 0):
        super(RawGzipMedia,self).__init__() 
        self.__filename = filename
        self.__fileObj = None
        self.__gzipFile = None
        self.__disksize = imagesizeBytes
        self.__zipLevel = zipLevel
        return 

    #starts the connection
    def open(self):
        #Note: it may also be recreated!
        #TODO: now it just doesn't zip at all
        self.__fileObj = open(self.__filename , "w+b")
        if self.__zipLevel:
            self.__gzipFile = gzip.GzipFile("tmpgzip", "wb" , self.__zipLevel , self.__fileObj)
        else: 
            self.__gzipFile = self.__fileObj
        

    def getMaxSize(self):
        return self.__disksize

    def reopen(self):
        self.close()
        self.open()

    def close(self):
        if self.__gzipFile:
            self.__gzipFile.close()
        if self.__fileObj:
            self.__fileObj.close()

    def flush(self):
        self.__gzipFile.flush()
        self.__fileObj.flush()
    
    def release(self):
        #TODO: refcounts should be added
        self.close()

    #reads data from image, returns data buffer
    def readImageData(self , offset , size):
        self.__fileObj.seek(offset)
        return self.__fileObj.read(size)

    #writes data to the container (as it was a disk)
    def writeDiskData(self, offset, data):
        self.__gzipFile.seek(offset)
        #Note: it doesn't support non-sequental writes!
        self.__gzipFile.write(data)
        self.flush()

    #reads data from the container (as it was a disk)
    def readDiskData(self , offset , size):
        self.__gzipFile.seek(offset)
        return  self.__gzipFile.read(size)

    #gets the overall image size available for writing. Note: it is subject to grow when new data is written
    def getImageSize(self):
        self.flush()
        return os.stat(self.__filename).st_size

    #sets the channel so the data may be sent simultaniously. Not implemented for now
    def setChannel(self):
        raise NotImplementedError
