"""
DefferedUploadFile
~~~~~~~~~~~~~~~~~

This module provides DefferedUploadFile class
A class to use as pipe between image reader and data uploader
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback

import threading
import Queue


class DefferedUploadFile(file):

    def __init__(self,  *args):
        self.__lock = threading.Lock()
        self.__queue = Queue.Queue(4)
        self.__cancelled = False
        self.__readPosition = 0
        #return super(DefferedFTPFile, self).__init__(*args)
        

    def read(self, size):
        """emulate read"""
        logging.debug("Requested " + str(size) + " bytes to transfer to upload")
        data = self.__queue.get()
        if data == None:
            return ""
        logging.debug("Transfering " + str(len(data)) + " bytes ")
        self.__readPosition = self.__readPosition  + len(data)
        return data

    def complete(self):
        logging.info("completing the transfer")
        self.__queue.put(None)

    def close(self):
        logging.debug("completing the transfer")
        self.__queue.put(None)

    def write(self, str):
        self.__queue.put(str)

    def cancel(self):
        self.__cancelled = True
        #empty the queue
        while self.__queue.empty()==False:
            self.__queue.get()

    def cancelled(self):
        return self.__cancelled

    def readinto(self):
        return super(DefferedUploadFile, self).readinto()
    def flush(self):
        return super(DefferedUploadFile, self).flush()
    def isatty(self):
        return super(DefferedUploadFile, self).isatty()
    
    def writelines(self, sequence_of_strings):
        return super(DefferedUploadFile, self).writelines(sequence_of_strings)
    def seek(self, offset, whence):
        return super(DefferedUploadFile, self).seek(offset, whence)
    def readline(self, size):
        return super(DefferedUploadFile, self).readline(size)
    def tell(self):
        return self.__readPosition 
    
    def readlines(self, size):
        return super(DefferedFTPFile, self).readlines(size)


