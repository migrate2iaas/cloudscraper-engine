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

class DefferedFTPFile(file):

    def __init__(self,  *args):
        self.__lock = threading.Lock()
        self.__queue = Queue.Queue(1)
        self.__cancelled = False
        #return super(DefferedFTPFile, self).__init__(*args)
        

    def read(self, size):
        """emulate read"""
        logging.debug("Requested " + str(size) + " bytes to transfer via FTP")
        data = self.__queue.get()
        if data == None:
            return ""
        logging.debug("Transfering " + str(len(data)) + " bytes via FTP")
        return data

    def complete(self):
        logging.debug("completing the transfer")
        self.__queue.put(None)

    def close(self):
        logging.debug("completing the transfer")
        self.__queue.put(None)

    def write(self, str):
        self.__queue.put(str)

    def cancel(self):
        self.__cancelled = True

    def cancelled(self):
        return self.__cancelled

    def readinto(self):
        return super(DefferedFTPFile, self).readinto()
    def flush(self):
        return super(DefferedFTPFile, self).flush()
    def isatty(self):
        return super(DefferedFTPFile, self).isatty()
    
    def writelines(self, sequence_of_strings):
        return super(DefferedFTPFile, self).writelines(sequence_of_strings)
    def seek(self, offset, whence):
        return super(DefferedFTPFile, self).seek(offset, whence)
    def readline(self, size):
        return super(DefferedFTPFile, self).readline(size)
    def tell(self):
        return super(DefferedFTPFile, self).tell()
    
    def readlines(self, size):
        return super(DefferedFTPFile, self).readlines(size)

def readThreadRoutine(ftp , filepath , fileobj , chunksize):
    try:
        logging.info("FTP transfer begin");
        ftp.storbinary("STOR " + filepath , fileobj , chunksize)
        logging.info("FTP transfer complete");
    except Exception as e:
         logging.warning("!Failed to upload data: %s", filepath)
         logging.warning("Exception = " + str(e)) 
         logging.error(traceback.format_exc())
         

class FtpUploadChannel(UploadChannel.UploadChannel):
    """channel to upload stuff via FTP"""

    def __init__(self , filepath , user  , password , hostname , resume=False):
        """constructor"""
        self.__filepath = filepath
        self.__user = user
        self.__password = password
        self.__hostname = hostname
        self.__resume = resume
        self.__chunkSize = 1*1024*1024
        self.__transferRate = 0
        self.__uploadSkippedSize = 0
        self.__uploadedSize  = 0
        logging.info ("Connecting host " + self.__hostname)
        self.__ftp = ftplib.FTP(self.__hostname , self.__user, self.__password) 
        self.__proxyFileObj = None
        self.__thread = None

        
        return


    
    def uploadData(self, extent):       
       """Note: should be sequental"""
       
       if self.__proxyFileObj == None:
           #reconnect
           self.__ftp = ftplib.FTP(self.__hostname , self.__user, self.__password) 
           self.__proxyFileObj = DefferedFTPFile()
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
                self.__thread.join()
            self.__proxyFileObj = None
        return

    def confirm(self):
        """confirm good upload. just close connection"""
        if self.__proxyFileObj:
            self.__proxyFileObj.complete()
        return self.__filepath

    def close(self):
        return
  

