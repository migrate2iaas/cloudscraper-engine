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

    def __init__(self , filename , imagesizeBytes):
        self.__filename = filename
        self.__fileObj = None
        self.__gzipFile = None
        self.__disksize = imagesizeBytes
        return 

    #starts the connection
    def open(self):
        #Note: it may also be recreated!
        self.__fileObj = open(self.__filename , "w+b")
        self.__gzipFile = gzip.GzipFile("tmpgzip", "wb" , 6 , self.__fileObj)
        

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
        self.__gzipFile.write(data)
        self.flush()

    #reads data from the container (as it was a disk)
    def readDiskData(self , offset , size):
        self.__gzipFile.seek(offset)
        return  self.__gzipFile.read(data)

    #gets the overall image size available for writing. Note: it is subject to grow when new data is written
    def getImageSize(self):
        self.flush()
        return os.stat(self.__filename).st_size

    #sets the channel so the data may be sent simultaniously. Not implemented for now
    def setChannel(self):
        raise NotImplementedError
