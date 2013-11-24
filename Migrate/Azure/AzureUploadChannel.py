"""
This file defines upload to Azure
"""

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
import traceback

import zlib
import gzip
import StringIO
import UploadChannel
import MultithreadUpoadChannel
from md5 import md5

from azure.storage import *






class AzureUploadTask(MultithreadUpoadChannel.UploadTask):
    def __init__(self, container , keyname, offset , size, data_getter, channel, alternative_source_bucket = None, alternative_source_keyname = None):
        self.__targetContainer = container
        self.__targetKeyname = keyname
        self.__targetSize = size
        self.__dataGetter = data_getter 
        self.__targetOffset = offset
        self.__alternativeKey = alternative_source_keyname
        self.__alternativeBucket = alternative_source_bucket

        super(AzureUploadTask).__init__(channel , size , offset)

   
    def getTargetKey(self):
        return self.__targetKeyname

    def getTargetContainer(self):
        return self.__targetContainer

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
        return False
        #return self.__alternativeKey != None and self.__alternativeBucket != None

    #TODO: find more generic way
    # sometimes source could be active too
    def setAlternativeUploadPath(self, alternative_source_bucket = None, alternative_source_keyname = None):
        return None

    #some dedup staff
    def getAlternativeKeyName(self):
        return ""

    def getAlternativeContainer(self):
        return ""

    def getAlternativeKey(self):
        """gets alternative key object"""
        return ""
        #return self.__targetBucket.get_key(self.__alternativeKey)



class AzureUploadChannel(MultithreadUpoadChannel.MultithreadUpoadChannel):
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

        self.__nullData = bytearray(self.__chunkSize)
        md5encoder = md5()
        md5encoder.update(self.__nullData)
        self.__nullMd5 = md5encoder.hexdigest()


        super(AzureUploadChannel,self).__init__(resulting_size_bytes , resume_upload , chunksize , upload_threads , queue_size)

 
    def initStorage(self):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """

        containers = self.__blobService.list_containers()
        container_found = False
        for container in containers:
            if container.name == self.__containerName:
                container_found = True

        if container_found == False:
            self.__blobService.create_container(self.__containerName)

        #create empty blob
        self.__blobService.put_blob(self.__containerName, self.__diskName, '', x_ms_blob_type='PageBlob' , x_ms_blob_content_length = self.getImageSize())
        
        logging.info("Succesfully created an upload channel to Azure container " + self.__storageAccountName  + " at " +  self.__containerName + "\\" + self.__diskName)
  

        return True

    def getUploadPath(self):
        """ gets the upload path identifying the upload: container/key """
        return  self.__storageAccountName + "/" +  self.__containerName + "/" + self.__diskName

    def createUploadTask(self , extent):
        """ 
        Protected method. Creates upload task to add it the queue 
        Returns UploadTask (or any of inherited class) object

        Args:
            extent: DataExtent - extent of the image to be uploaded. Its size and offset should be integral of chunk size. The only exception is last chunk. 

        """
        start = extent.getStart()
        size = extent.getSize()
        data = extent.getData()

        logging.debug("Creating upload task for offset:" + str(start) + " and size "+str(size) + " chunk: " +  str(self.getTransferChunkSize()))

        return AzureUploadTask(self.__containerName,  self.__diskName , start , size , data, self)

    
    def uploadChunk(self, uploadtask):
        """
        Protected. Uploads one chunk of data
        Called by the worker thread internally.
        Could throw any non-recoverable errors
        """
        container_name = uploadtask.getTargetContainer()
        diskname = uploadtask.getTargetKey()

        offset = uploadtask.getUploadOffset()
        size = uploadtask.getUploadSize()
        
        if uploadtask.getDataMd5() == self.__nullMd5:
           if uploadtask.getData() == self.__nullData:
               uploadtask.notifyDataSkipped()
               return True

        data = uploadtask.getData()

        chunk_range = 'bytes={}-{}'.format(offset, offset + size - 1)

        try:
            self.__blobService.put_page(container_name, diskname , data , x_ms_range=chunk_range , x_ms_page_write="update")
        except Exception as e:
            logging.warning("!Failed to upload data: %s/%s range %s", str(container_name), diskname , chunk_range)
            logging.warning("Exception = " + str(e)) 
            logging.error(traceback.format_exc())
            raise
            
        logging.debug("%s/%s range %s uploaded", str(container_name), diskname , chunk_range );
        uploadtask.notifyDataTransfered()
        return True
    
    
    def confirm(self):
        """
        Confirms good upload
        """
        if self.unsuccessfulUpload():
            logging.error("!!!ERROR: there were upload failures. Please, reupload by choosing resume upload option!") 
            return None
        
        blobs = self.__blobService.list_blobs(self.__containerName , self.__diskName) 
        for blob in blobs:
            if blob.name == self.__diskName:
                return blob.url
        logging.error("!!!ERROR: no blob matching the disk name found!")
        return None

   
