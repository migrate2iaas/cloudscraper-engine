"""
MultithreadUpoadChannel
~~~~~~~~~~~~~~~~~

This module provides MultithreadUpoadChannel class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import UploadChannel
import threading

import Queue
import DataExtent
import time
import tempfile
import warnings
import datetime

import sys
import os
import MigrateExceptions
import subprocess
import re

class UploadTask(object):
    def __init__(self , channel , size , offset):
        """ Add any cloud related initialization here"""
        self.__channel = channel
        self.__targetSize = size
        self.__targetOffset = offset

    def notifyDataTransfered(self):
        if self.__channel:
            self.__channel.notifyDataTransfered(self.__targetOffset, self.__targetSize)

    def notifyDataSkipped(self):
        if self.__channel:
           self.__channel.notifyDataSkipped(self.__targetOffset, self.__targetSize)

    def notifyDataTransferError(self):
        if self.__channel:
            self.__channel.notifyTransferError(self.__targetOffset, self.__targetSize)

    def getUploadSize(self):
        return self.__targetSize

    def getUploadOffset(self):
        return self.__targetOffset


class UploadThread(threading.Thread):
    """thread making all uploading works"""
    def __init__(self , queue , thread_id , skip_existing = False , channel = None , copy_similar = True,  retries = 5):
        """
        Constructor
        
        Args:
            queue: Queue - queue to fetch tasks from
            thread_id: int - any id identifying the thread. Needed for debugging reasons only
            skip_existing: bool - whether to reupload existing chunks or not
            channel: UploadChannel - channel object for callbacks
            copy_similar: bool - whether to copy similar blocks from the storage nearest to the destination
            retries: int - number of retries till download is treated as failed
        """
        self.__uploadQueue = queue
        #thread id , for troubleshooting purposes
        self.__threadId = thread_id
        self.__skipExisting = skip_existing
        self.__maxRetries = retries
        self.__copySimilar = copy_similar
        self.__channel = channel
        
        super(UploadThread,self).__init__()

    def run(self):
        while 1:

            uploadtask = self.__uploadQueue.get()
            
            # means it's time to exit
            if uploadtask == None:
                return

            failed = True
            retries = 0
            while retries < self.__maxRetries:
                retries = retries + 1

                try:
                    self.__channel.uploadChunk(uploadtask)
                except Exception as e:
                    # reput the task into the queue
                    logging.warning("!Failed to upload data, making a retry..." )
                    logging.warning("Exception = " + str(e)) 
                    logging.error(traceback.format_exc()) 
                    continue

                logging.debug("Upload thread "+str(self.__threadId)+ " set queue task done");
                self.__uploadQueue.task_done()
                failed = False
                break

            if failed:
                logging.error("!!! ERROR failed to upload data!")
                self.__uploadQueue.task_done()
                uploadtask.notifyDataTransferError()
            


class MultithreadUpoadChannel(UploadChannel.UploadChannel):
    """
    Base upload channel implementation providing basic facilities for multithreaded upload. 
    Cloud-specific implementations could override the following methods:
    
    Need to override:
        -initStorage()
        -getUploadPath()
        -createUploadTask()
        -confirm()
        -uploadChunk()

    Protocol chain:

        channel = MultithreadUpoadChannel.MultithreadUpoadChannel(...) #any inherited class
        try:
            channel.initStorage()
            for data_ext in data_extents:
                channel.uploadData(data_ext)
            channel.waitTillUploadComplete()
            channel.confirm()
        except Exception as ex:
            ...
        finally:
            channel.close()
        


    """

    def __init__(self, result_disk_size_bytes , resume_upload = False , chunksize=10*1024*1024 , upload_threads=4 , queue_size=16 , sync_every_requests = 128):
        """
        Inits the default multithreaded upload channel

        Args:
            result_disk_size_bytes - the size of resulting disk object
            resume_upload - whether the upload is reumed
            chunksize - the size of one upload chunk
            upload_threads - number of simulanious upload threads
            queue_size - the size of upload queue
            sync_every_requests - syncs threads (wait when they are complete every requests)
        """
        self.__uploadQueue = Queue.Queue(queue_size)
        self.__statLock = threading.Lock()
        self.__prevUploadTime = None 
        self.__transferRate = 0

        self.__chunkSize = chunksize 
        self.__resumeUpload = resume_upload
        self.__errorUploading = False

        self.__uploadedSize = 0
        self.__uploadSkippedSize = 0
        self.__overallSize = 0
        self.__uploadThreadsCount = upload_threads
        self.__alreadyUploaded = 0
        self.__uploadedPropertyChecked = False

        self.__resultDiskSizeBytes = result_disk_size_bytes

        # counters representing the number of requests to be executed till wait to sync them
        self.__syncEvery = sync_every_requests
        self.__syncCount = 0

        #dictionary by the start of the block
        self.__fragmentDictionary = dict()
        #initializing a number of threads, they are stopping when see None in queue job
        self.__workThreads = list()
        i = 0
        while i < self.__uploadThreadsCount:
            thread = UploadThread(self.__uploadQueue , i , self.__resumeUpload , self)
            thread.start()
            self.__workThreads.append(thread )
            i = i + 1
        return

    ############ ------- TO OVERRIDE -------- ###########


    def initStorage(self, init_data_link=""):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """
        raise NotImplementedError

    def getUploadPath(self):
        """
        Gets string representing the channel, e.g. Amazon bucket and keyname that could be used for upload.
        Needed mainly in diagnostics purposes
        """
        raise NotImplementedError

    def createUploadTask(self , extent):
        """ 
        Protected method. Creates upload task to add it the queue 
        Returns UploadTask (or any of inherited class) object

        Args:
            extent: DataExtent - extent of the image to be uploaded. Its size and offset should be integral of chunk size. The only exception is last chunk. 

        """
        raise NotImplementedError

    
    def confirm(self):
        """
        Registers the image in cloud
        Note, call confirm() only after waitTillUploadComplete() to ensure the upload task is complete.

        Return:
             Cloud uploaded disk image identifier that could be passed to Cloud API to create new server: str - in case of success or
             None - in case of failure
        """
        raise NotImplementedError

    def uploadChunk(self , uploadtask):
        """
        Protected. Uploads one chunk of data
        Called by the worker thread internally.
        Could throw any non-recoverable errors
        """
        raise NotImplementedError

    ###########$ ------- STANDARD IMPL ------- $$$$$$$$$$$$

    def getImageSize(self):
        """
        Gets image data size to be uploaded
        """
        return self.__resultDiskSizeBytes

    def uploadData(self, extent):       
       """ 
       Adds data extent to the upload queue. Use waitTillUploadComplete() to wait till all data is uploaded
       it should be used from single thread

       Args:
            extent: DataExtent - extent of the image to be uploaded. Its size and offset should be integral of chunk size. The only exception is last chunk. 

       """
       # loads data already uploaded on the fiest run
       if self.__resumeUpload and (self.__alreadyUploaded == 0) and self.__uploadedPropertyChecked == False:
           self.__uploadedPropertyChecked = True
           if self.loadDiskUploadedProperty():
               self.__alreadyUploaded = self.getDiskUploadedProperty()

       start = extent.getStart()
       size = extent.getSize()
       
       uploadtask = self.createUploadTask(extent)        

       self.__syncCount = self.__syncCount + 1

       if self.__syncCount > self.__syncEvery:
             self.__uploadQueue.join()
             if self.unsuccessfulUpload():
                 return False
             self.notifyThreadsSynced(start)
             self.__syncCount = 0

       self.__uploadQueue.put( uploadtask )

       # TODO: add something (identifier?) from uploadtask here
       self.__fragmentDictionary[start] = (size) 

       return (self.unsuccessfulUpload() == False)


    def getTransferChunkSize(self):
        """ 
        Gets the size of one chunk of data transfered by the each request, 
        The data extent is better to be aligned by the integer of chunk sizes

        See UploadChannel.UploadChannel for more info
        """
        return self.__chunkSize

    
    def isUploadResumed(self):
        """ 
        Checks if upload is resumed one
        """
        return self.__resumeUpload


    def unsuccessfulUpload(self):
        """
        Checks if there were unrecoverable errors during the upload
        """
        return self.__errorUploading

   
    def getDataTransferRate(self):
        """ 
        Return:
            Approx number of bytes transfered in seconds

        See UploadChannel.UploadChannel for more info

        Note: not implemented for now
        """
        return self.__transferRate

    #  overall data skipped from uploading if resume upload is set
    def notifyDataSkipped(self , offset, skipped_size):
        """ For internal use only by the worker threads    """
        with self.__statLock:
            self.__uploadSkippedSize = self.__uploadSkippedSize + skipped_size

    # gets overall data skipped from uploading if resume upload is set
    def getOverallDataSkipped(self):
        """ Gets overall data skipped  """
        return self.__uploadSkippedSize

    def notifyTransferError(self, start , size):
        """ For internal use only by the worker threads   """
        self.__errorUploading = True
        return

    def notifyDataTransfered(self , offset, transfered_size):
        """ For internal use only by the worker threads   """

        now = datetime.datetime.now()
        if self.__prevUploadTime:
            delta = now - self.__prevUploadTime
            if delta.seconds:
                self.__transferRate = transfered_size/delta.seconds
        self.__prevUploadTime = now
        with self.__statLock:
            self.__uploadedSize = self.__uploadedSize + transfered_size
            self.__alreadyUploaded = self.__uploadSkippedSize + self.__uploadedSize

    
    def notifyThreadsSynced(self, offset):
        """ notifies all data till offset was transferred. override if this info is needed for updating """
        self.updateDiskUploadedProperty(offset)
   

    def getOverallDataTransfered(self):
        """ gets the overall size of data actually uploaded in bytes """
        return self.__uploadedSize 

    def waitTillUploadComplete(self):
        """
        Client calls this method to wait till all async upload is complete
        """
        logging.debug("Upload complete, waiting for threads to complete");
        self.__uploadQueue.join()
        maxoffset = max(self.__fragmentDictionary.iterkeys())
        self.notifyThreadsSynced( maxoffset + self.__fragmentDictionary[maxoffset] )
        return

    # NOTE: there could be a concurency error when one threads adds the extend while other thread closes all the connections
    # so there would be extent request in the thread but all threads were close. So then waitTillUploadComplete hang
    def close(self):
        """Closes the channel, sending all upload threads signal to end their operation"""
        logging.debug("Closing the upload threads, End signal message was sent")
        for thread in self.__workThreads:
            self.__uploadQueue.put( None )

    def getDiskUploadedProperty(self):
        """
        Returns amount of data already uploaded as it saved in the cloud storage
        This data could be loaded from the disk object on cloud side which channel represents
        """
        return 0  

    def loadDiskUploadedProperty(self):
        return False

    def updateDiskUploadedProperty(self , newval):
        """
        function to set property to the disk in cloud on the amount of data transfered
        Returns nothing
        """
        logging.error("doesn't update uploaded property " + str(newval));