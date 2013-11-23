# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import sys

sys.path.append('.\..')
sys.path.append('.\..\Amazon')
sys.path.append('.\..\Windows')
sys.path.append('.\..\ElasticHosts')
sys.path.append('.\..\Azure')

sys.path.append('.\Windows')
sys.path.append('.\Amazon')
sys.path.append('.\ElasticHosts')
sys.path.append('.\Azure')


import os 
import threading
import Queue
import DataExtent
import time
import tempfile
import warnings
import logging

import sys
import os
import subprocess
import re

import logging
import threading 
import datetime

import zlib
import gzip
import StringIO
import UploadChannel

from azure.storage import *






class UploadQueueTask:
    def __init__(self, container , keyname, offset , size, data_getter, channel, alternative_source_bucket = None, alternative_source_keyname = None):
        self.__channel = channel
        self.__targetContainer = container
        self.__targetKeyname = keyname
        self.__targetSize = size
        self.__dataGetter = data_getter 
        self.__alternativeKey = alternative_source_keyname
        self.__alternativeBucket = alternative_source_bucket

    def notifyDataTransfered(self):
        if self.__channel:
            self.__channel.notifyDataTransfered(self.__targetSize)

    def notifyDataSkipped(self):
        if self.__channel:
           self.__channel.notifyDataSkipped(self.__targetSize)

    def notifyDataTransferError(self):
        if self.__channel:
            self.__channel.notifyTransferError(self.__targetBucket , self.__targetKeyname, self.__targetSize)

    def getTargetKey(self):
        return self.__targetKeyname

    def getTargetContainer(self):
        return self.__targetContainer

    def getSize(self):
        return self.__targetSize

    def getData(self):
        return str(self.__dataGetter)[0:self.__targetSize]

    def getDataMd5(self):
        md5encoder = md5()
        data = self.getData()
        md5encoder.update(data)
        return md5encoder.hexdigest()

    # specifies the alternitive availble path. 
    # alternative source seem to be interesting concept. get something from another place. maybe deferred
    def isAlternitiveAvailable(self):
        return self.__alternativeKey != None and self.__alternativeBucket != None

    #TODO: find more generic way
    # sometimes source could be active too
    def setAlternativeUploadPath(self, alternative_source_bucket = None, alternative_source_keyname = None):
        self.__alternativeKey = alternative_source_keyname
        self.__alternativeBucket = alternative_source_bucket

    #some dedup staff
    def getAlternativeKeyName(self):
        return self.__alternativeKey

    def getAlternativeContainer(self):
        return self.__alternativeBucket

    def getAlternativeKey(self):
        """gets alternative key object"""
        return self.__targetBucket.get_key(self.__alternativeKey)




class AzureUploadThread(threading.Thread):
    """thread making all uploading works"""

    def __init__(self , queue , threadid , skipexisting = False , blobservice = None, channel = None , copysimilar = True,  retries = 3):
        self.__uploadQueue = queue
        #thread id , for troubleshooting purposes
        self.__threadId = threadid
        self.__skipExisting = skipexisting
        self.__maxRetries = retries
        self.__copySimilar = copysimilar
        self.__blobService = blobservice
        super(S3UploadThread,self).__init__()

    def run(self):
        while 1:
            uploadtask = self.__uploadQueue.get()
            
            # means it's time to exit
            if uploadtask == None:
                return

            container = uploadtask.getTargetContainer()
            diskname = uploadtask.getTargetKey()
            
            offset = ploadtask.getOffset()
            size = uploadtask.getSize()
            data = uploadtask.getData()
            
            md5_hexdigest = uploadtask.getDataMd5()

            chunk_range = 'bytes={}-{}'.format(offset, offset + size - 1)

            retries = 0

            while retries < self.__maxRetries:
                retries = retries + 1
                try:
                    self.__blobService.put_page(container_name, diskname , data , x_ms_range=chunk_range , x_ms_page_write="update")
                except Exception as e:
                    logging.warning("!Failed to upload data: %s/%s , making a retry...", str(container_name), diskname )
                    logging.warning("Exception = " + str(e)) 
                    logging.error(traceback.format_exc())
                    failed = True
                    continue

                logging.debug("Upload thread " + str(self.__threadId) + " set queue task done");
                self.__uploadQueue.task_done()
                failed = False
            
            if failed:
                logging.error("!!! ERROR failed to upload data: %s/%s!", str(container_name), diskname )
                uploadtask.notifyDataTransferError()
                self.__uploadQueue.task_done()
                    
            


class AzureUploadChannel(UploadChannel.UploadChannel):
    """
    Upload channel for Azure implementation
    Implements multithreaded fie upload to Windows Azure 
    """

    def __init__(self , storageacc , accesskey , resulting_size_bytes, container_name  , diskname , resume_upload = False , chunksize=10*1024*1024 , upload_threads=4 , queue_size=16):
        """constructor"""
        self.__chunkSize = chunksize
        self.__storageAccountName = storageacc
        self.__containerName = container_name
        self.__diskName = diskname
        self.__blobService = BlobService(account_name=storageacc, account_key=accesskey)
        self.__resumeUpload = resume_upload

        #statistics
        self.__overallSize = 0
        self.__transferRate = 0
        self.__prevUploadTime = datetime.datetime.now()
        self.__statLock = threading.Lock()
        self.__uploadedSize = 0
        self.__errorUploading = True

        containers = self.__blobService.list_containers()
        container_found = False
        for container in containers:
            if container.name == container_name:
                container_found = True

        if container_found == False:
            self.__blobService.create_container(container_name)

        #create empty blob
        self.__blobService.put_blob(container_name, diskname, '', x_ms_blob_type='PageBlob' , x_ms_blob_content_length = resulting_size_bytes)

        self.__workThreads = list()
        i = 0
        while i < upload_threads:
            thread = AzureUploadThread(self.__uploadQueue , i , self.__resumeUpload , self.__blobService , self)
            thread.start()
            self.__workThreads.append(thread)
            i = i + 1
        return

        logging.info("Succesfully created an upload channel to Azure container " + self.__storageAccountName  + " at " +  container_name + "\\" + diskname)

        return

    def getUploadPath(self):
        """ gets the upload path identifying the upload: bucket/key """
        return  self.__storageAccountName + "/" +  self.__containerName + "/" + self.__diskName

    # this one is async
    def uploadData(self, extent):       
       """ 
       Uploads data extent

       See UploadChannel.UploadChannel for more info
       """
       start = extent.getStart()
       size = extent.getSize()
       
       skipupload = False
              
       uploadtask = UploadQueueTask(self.__containerName , self.__diskName , start , size, extent.getData() , self )
       
       if uploadtask.getDataMd5() == self.__nullMd5:
           if uploadtask.getData() == self.__nullData:
                skipupload = True  

       if skipupload == False:
            self.__uploadQueue.put( uploadtask )
       # treating overall size as maximum size
       if self.__overallSize < start + size:
           self.__overallSize = start + size       

       return 

    def getTransferChunkSize(self):
        """ 
        Gets the size of one chunk of data transfered by the each request, 
        The data extent is better to be aligned by the integer of chunk sizes

        See UploadChannel.UploadChannel for more info
        """
        return self.__chunkSize

   
    def getDataTransferRate(self):
        """ 
        Return:
            Approx number of bytes transfered in seconds

        See UploadChannel.UploadChannel for more info

        Note: not implemented for now
        """
        return self.__transferRate

    #  overall data skipped from uploading if resume upload is set
    def notifyDataSkipped(self , skipped_size):
        """ For internal use only by the worker threads    """
        with self.__statLock:
            self.__uploadSkippedSize = self.__uploadSkippedSize + skipped_size

    # gets overall data skipped from uploading if resume upload is set
    def getOverallDataSkipped(self):
        """ Gets overall data skipped  """
        return self.__uploadSkippedSize

    def notifyTransferError(self, bucket , keyname, size):
        """ For internal use only by the worker threads   """
        self.__errorUploading = True
        return

    def notifyDataTransfered(self , transfered_size):
        """ For internal use only by the worker threads   """
        now = datetime.datetime.now()
        if self.__prevUploadTime:
            delta = now - self.__prevUploadTime
            if delta.seconds:
                self.__transferRate = transfered_size/delta.seconds
        self.__prevUploadTime = now
        with self.__statLock:
            self.__uploadedSize = self.__uploadedSize + transfered_size

    
    def getOverallDataTransfered(self):
        """ #gets the overall size of data actually uploaded in bytes """
        return self.__uploadedSize 

    def waitTillUploadComplete(self):
        """Waits till upload completes"""
        logging.debug("Upload complete, waiting for threads to complete");
        self.__uploadQueue.join()
        return

    # 
    def confirm(self):
        """
        Confirms good upload. uploads resulting xml describing VM container import to S3 
        
        Return
             Link to XML uploaded
        """
        # generate the XML file then:

        if self.__errorUploading:
            logging.error("!!!ERROR: there were upload failures. Please, reupload by choosing resume upload option!") 
            return None
