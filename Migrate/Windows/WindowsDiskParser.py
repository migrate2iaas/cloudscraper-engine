import sys
sys.path.append(sys.path[0]+'\\..')

import subprocess
import re

import WindowsVolumeTransferTarget


import win32file
import win32event
import win32con
import win32security
import win32api
import pywintypes
import ctypes
import winioctlcon
import struct
import ntsecuritycon

import logging

#TODO: to implement base class
class WindowsDiskParser(object):
    """The parser of Windows container"""

    def __init__(self , backingStore , mbrId):
        self.__backingStore = backingStore
        #TODO: check if it supports interfaces needed

        self.__freeSize = self.__backingStore.getSize()
        self.__wholeSize = self.__backingStore.getSize()
        self.__diskNumber =  self.__backingStore.getDeviceNumber()
        self.__mbrId = int(mbrId)

    #TODO: enum of existing targets\volumes

    # the disk is formated and new volume is generated
    def createTransferTarget(self, size):

        scriptpath = "diskpart_format.tmp"
        scrfile = open(scriptpath, "w+")
        script = "select disk " + str(self.__diskNumber) 
        if self.__mbrId:
            mbridstr = hex(self.__mbrId)[2:] # # we substract first 0x symbols
            while len(mbridstr) < 8:
                mbridstr = "0"+mbridstr # and add some 0s at the beggining
            script = script.__add__("\nUNIQUEID DISK ID="+mbridstr);
        script = script.__add__("\nCREATE PARTITION PRIMARY");
        if size:
            sizemb = int(size/1024/1024)
            if sizemb % (1024*1024):
                sizemb = sizemb + 1
            script = script.__add__(" size=" + str(sizemb))
        script = script.__add__("\nACTIVE");
        script = script.__add__("\nONLINE VOLUME");
        script = script.__add__("\nFORMAT FS=NTFS REVISION=6.00 QUICK");
        script = script.__add__("\nDETAIL PARTITION");
        scrfile.write(script);
        scrfile.close()

        try:
            output = subprocess.check_output("diskpart /s \"" + scriptpath +"\"" , shell=True);
        except subprocess.CalledProcessError as ex:
            logging.error("!!!ERROR: Cannot create target volume")
            logging.error("diskpart failed" + ex.output)
            raise

        match = re.search('Partition ([0-9])+',output)
        if match == None:
            logging.error("!!!ERROR: Cannot create target volume");
            logging.error("Bad output from the diskpart utility! Output: " + output);
            raise EnvironmentError
        partno = int(match.group(1))
        logging.debug("Openning windows transfer target: %s" , "\\\\?\\GLOBALROOT\\Device\\Harddisk"+str(self.__diskNumber)+"\\Partition"+str(partno));
        return WindowsVolumeTransferTarget.WindowsVolumeTransferTarget("\\\\?\\GLOBALROOT\\Device\\Harddisk"+str(self.__diskNumber)+"\\Partition"+str(partno))
        
    # TODO: move the data\metadata ditinction here    

    # to write directly the partitioning schemes
    def writeRawMetaData(self, metadataExtents):
        return

    # to read the partitioning schemes
    def readRawMetaData(self, metadataExtent):
        return

    # to write directly the partitioning schemes
    def readVolumeData(self, dataExtent):
        return

    # to read the partitioning schemes
    def writeVolumeData(self, dataExtent):
        return

    # to write directly the partitioning schemes
    def readVolumeMetadata(self, dataExtent):
        return

    # to read the partitioning schemes
    def writeVolumeMetadata(self , dataExtent):
        return