"""
CloudSigmaUploadChannel
~~~~~~~~~~~~~~~~~

This module provides CloudSigmaUploadChannel class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys
import os 
import logging
import traceback

import threading
import Queue
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

import requests
import zlib
import gzip
import StringIO
import UploadChannel
import MultithreadUpoadChannel
import urlparse

import base64
import math
from md5 import md5

from cloudsigma.resource import *


class CloudsigmaUploadTask(MultithreadUpoadChannel.UploadTask):
    def __init__(self, channel , api_endpoint , username , password ,  drive_uuid , total_size , chunk_number, chunk_size , data_getter ):
        """

        """
        self.__driveUuid = drive_uuid
        self.__totalSize = total_size
        self.__chunkSize = chunk_size
        self.__chunkNumber = chunk_number
        self.__username = username
        self.__password = password
        self.__dataGetter = data_getter
        self.__apiEndpoint = api_endpoint
        super(CloudsigmaUploadTask, self).__init__(channel , chunk_size , self.__chunkNumber*chunk_size)

    def getUploadUrl(self):
        url = '/{0}/{1}/upload/'.format('drives', self.__driveUuid)
        if url[0] == '/':
               url = url[1:]
        upload_url = urlparse.urljoin(self.__apiEndpoint, url)
        return upload_url

    def getExtraHeaders(self):
        kwargs = {
            'auth': (self.__username, self.__password),
            'headers': {
                'user-agent': 'CloudSigma turlo client',
            }
        }
        return kwargs

    def getParms(self):
        resumable_js_data = {'resumableChunkNumber': str(self.__chunkNumber),
                                'resumableChunkSize': str(self.__chunkSize),
                                'resumableTotalSize': str(self.__totalSize),
                                'resumableIdentifier': self.__driveUuid,
                                'resumableFilename': self.__driveUuid
                                }
        return resumable_js_data

    def getUploadData(self):
        return str(self.__dataGetter)

    def getChunkNumber(self):
        return self.__chunkNumber

    def getChunkSize(self):
        """
        Gets expected chunksize. It could be smaller for the last chunk.
        Note: use len( getUploadData() ) to determine the actual size
        """
        return self.__chunkSize

    def getChunkOffset(self):
        return self.__chunkSize


#got tons of code from pyclodsigma resumable_upload.py
class CloudSigmaUploadResource (ResourceBase):
    """auxillary class to make pycloudsigma happy"""
    resource_name = 'initupload'
    
    def __init__(self , **generic_client_kwargs):
        """constructor"""
        self.generic_client_kwargs = generic_client_kwargs or {}
        super(CloudSigmaUploadResource, self).__init__(**self.generic_client_kwargs)


class CloudSigmaUploadChannel(MultithreadUpoadChannel.MultithreadUpoadChannel):
    """Uploads data to CloudSigma"""
    

    def __init__(self, result_disk_size_bytes , region , login , password, drive_name=None, drive_uuid=None , resume_upload = False , chunksize=10*1024*1024 , upload_threads=4 , queue_size=16 , generic_cloudsigma_kwargs=None):
        """
        Inits cloudsigma:

        TODO: describe parms
        """
        self.__driveUuid = drive_uuid
        
        base_kwargs = generic_cloudsigma_kwargs or {}

        if drive_name:
            self.__driveName = drive_name
        else:
            self.__driveName = os.environ['COMPUTERNAME']

        self.__region = region
        self.__login = login
        self.__password = password
        self.__apiEndpoint = "https://"+self.__region+".cloudsigma.com/api/2.0/"

        base_kwargs.update({'api_endpoint':self.__apiEndpoint , 'password':self.__password , 'username':self.__login})
        
        self.__cloudsigmaResource = CloudSigmaUploadResource(**base_kwargs)
        self.__cloudsigmaDrive = Drive(**base_kwargs)
        self.__driveMedia = 'disk'

        #TODO: maybe to add these to cloudoptions seems ok
        

        super(CloudSigmaUploadChannel,self).__init__(result_disk_size_bytes , resume_upload , chunksize , upload_threads , queue_size)

    ############ ------- TO OVERRIDE -------- ###########


    def initStorage(self, init_data_link=""):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """

        drive = None

        if self.isUploadResumed() and self.__driveUuid:
            drive = self.__cloudsigmaDrive.get(self.__driveUuid)
            drive_size = drive['size']
            if drive_size != self.getImageSize():
                logging.error("!ERROR: could not reupload drive. Original and cloud drive size mismatch")
                #TODO: throw appropriate error. See ElasticHosts
                return 

        if not drive:
            create_data = {
                'name': self.__driveName or 'Upload_{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.utcnow()),
                'media': self.__driveMedia,
                'size': self.getImageSize()
            }
            drive = self.__cloudsigmaResource.create(create_data)
            self.__driveUuid = drive['uuid']

        return True

    def getUploadPath(self):
        """
        Gets string representing the channel, e.g. Amazon bucket and keyname that could be used for upload.
        Needed mainly in diagnostics purposes
        """
        return self.__driveUuid

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

        chk_number = int(start / self.getTransferChunkSize())
        chunk_size = self.getTransferChunkSize()
        
        return CloudsigmaUploadTask(self,  self.__apiEndpoint , self.__login , self.__password , self.__driveUuid , self.getImageSize() , chk_number , chunk_size , data)

    
    def uploadChunk(self, uploadtask):
        """
        Protected. Uploads one chunk of data
        Called by the worker thread internally.
        Could throw any non-recoverable errors
        """
        upload_url = uploadtask.getUploadUrl()
        kwargs = uploadtask.getExtraHeaders()
        resumable_js_data = uploadtask.getParms()
        chunk_offset = uploadtask.getChunkOffset()
        chunk_number = uploadtask.getChunkNumber()

        res = requests.get(upload_url, params=resumable_js_data, **kwargs)
        if 199 < res.status_code < 300:
            logging.debug('Chunk #{0} offset {1} already uploaded'.format(chunk_number, chunk_offset))
            uploadtask.notifyDataSkipped()
            return
        
        # getting data after need of upload is confirmed to reduce the source server resource use
        file_data = uploadtask.getUploadData();
        resumable_js_data_multipart = resumable_js_data.items() +[('file', str(file_data))]

        res = requests.post(upload_url, files=resumable_js_data_multipart, **kwargs)
        if 199 < res.status_code < 300:
            logging.debug('Chunk #{0} offset {1} size {2} finished uploading'.format(chunk_number, chunk_offset, len(file_data)))
            uploadtask.notifyDataTransfered()
            return
        else:
            logging.error('Wrong status {0} returned for request '
                            '{1}:{2}:{3}. Response body is:'
                            '\n{4}'.format(res.status_code, chunk_number, chunk_offset, len(file_data), res.text))
            res.raise_for_status()
            

    
    
    def confirm(self):
        """
        Registers the image in cloud
        Note, call confirm() only after waitTillUploadComplete() to ensure the upload task is complete.

        Return:
             Cloud uploaded disk image identifier that could be passed to Cloud API to create new server: str - in case of success or
             None - in case of failure
        """
        if self.unsuccessfulUpload():
            logging.error("!!!ERROR: there were upload failures. Please, reupload by choosing resume upload option!") 
            return None
        return self.__driveUuid
