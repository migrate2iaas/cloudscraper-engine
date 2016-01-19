"""
FtpUploadChannel
~~~~~~~~~~~~~~~~~

This module provides FtpUploadChannel class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys
sys.path.append('.\..')
sys.path.append(sys.path[0]+'\\..')

import logging
import traceback
import ftplib
import threading
import UploadChannel
import Queue
import DataExtent
import socket
from DefferedUploadFile import DefferedUploadFile




def readThreadRoutine(ftp , filepath , fileobj , chunksize):
    try:
        logging.info("FTP transfer begin");
        ftp.storbinary("STOR " + filepath , fileobj , chunksize)
        logging.info("FTP transfer complete");
    except Exception as e:
         logging.warning("!Failed to upload data: %s", filepath)
         logging.warning("Exception = " + str(e)) 
         logging.error(traceback.format_exc())
         fileobj.cancel()
         

class FtpUploadChannel(UploadChannel.UploadChannel):
    """channel to upload stuff via FTP"""

    def __init__(self , filepath , user  , password , hostname , resume=False):
        """constructor"""
        self.__filepath = filepath
        self.__user = user
        self.__password = password
        self.__hostname = hostname
        self.__resume = resume
        self.__chunkSize = 512*1024
        self.__transferRate = 0
        self.__uploadSkippedSize = 0
        self.__uploadedSize  = 0
        logging.info ("Connecting host " + self.__hostname)
        self.__ftp = ftplib.FTP(self.__hostname , self.__user, self.__password) 
        self.__proxyFileObj = None
        self.__thread = None
        self.__timeout = 60*5

        
        return


    
    def uploadData(self, extent):       
       """Note: should be sequental"""
       
       if self.__proxyFileObj == None:
           #reconnect
           self.__ftp = ftplib.FTP(self.__hostname , self.__user, self.__password) 
           self.__proxyFileObj = DefferedUploadFile()
           self.__thread = threading.Thread(target = readThreadRoutine, args=(self.__ftp,self.__filepath,self.__proxyFileObj,self.__chunkSize,) )
           self.__thread.start()
       
       self.__proxyFileObj.write(extent.getData())

       self.__uploadedSize = self.__uploadedSize + extent.getSize()

       return self.__proxyFileObj.cancelled() == False

    def getUploadPath(self):
        return self.__filepath

    def getTransferChunkSize(self):
        """gets the size of one chunk of data transfered by the each request, the data extent is better to be aligned by the integer of chunk sizes """
        return self.__chunkSize

    def getDataTransferRate(self):
        """returns float: number of bytes transfered in seconds"""
        return self.__transferRate

    def notifyDataSkipped(self , skipped_size):
        """ overall data skipped from uploading if resume upload is set"""
        self.__uploadSkippedSize = self.__uploadSkippedSize + skipped_size

    def getOverallDataSkipped(self):
        """gets overall data skipped from uploading if resume upload is set"""
        return self.__uploadSkippedSize

  
    def getOverallDataTransfered(self):
        """gets the overall size of data uploaded"""
        return self.__uploadedSize 

    def waitTillUploadComplete(self):
        if self.__proxyFileObj:
            self.__proxyFileObj.complete()
            if self.__thread:
                self.__thread.join(self.__timeout)
                if self.__thread.isAlive():
                    logging.warn("! FTP connection hanged, terminating FTP connection");
                    #Note, there is no official thread terminate rountine. it's ok to just ignore it for now
            self.__proxyFileObj = None
        return

    def confirm(self):
        """confirm good upload. just close connection"""
        return self.__filepath

    def close(self):
        return
  

