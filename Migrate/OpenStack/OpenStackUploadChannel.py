"""
This file defines upload to OpenStack image service
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------


import sys

sys.path.append('.\..')
sys.path.append('.\..\OpenStack')

sys.path.append('.\OpenStack')


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

import datetime
import traceback

import zlib
import gzip
import StringIO
import UploadChannel
import MultithreadUpoadChannel
from md5 import md5
import requests
from DefferedUploadFile import DefferedUploadFile
import threading

import novaclient
import keystoneclient.v2_0.client as ksclient
from keystoneclient.auth.identity import v2
from keystoneclient import session
from novaclient import client
import glanceclient
import glanceclient.v2.client as glclient

import tarfile
import StringIO
import io

def glanceUploadThreadRoutine(glance, proxyFileObj, image, size, tarify, chunksize):
    try:
        proxy_file = proxyFileObj
        logging.info(">>> Image transfer being");
        glance.images.upload(image.id,proxy_file)
        logging.info(">>> Image transfer complete");
    except Exception as e:
         logging.warning("!Failed to upload data")
         logging.warning("Exception = " + str(e)) 
         logging.error(traceback.format_exc())
         proxyFileObj.cancel()



class OpenStackUploadChannel(UploadChannel.UploadChannel):
    """
    Upload channel for Azure implementation
    Implements multithreaded fie upload to Windows Azure 
    """

    def __init__(self, result_disk_size_bytes, server_url , tennant_name , username , password, disk_format = "vhd", image_name=None, resume_upload = False , chunksize=64*1024 , upload_threads=1 , queue_size=1 , container_format="bare"):
        """constructor"""
        keystone = ksclient.Client(auth_url=server_url,   username=username, password=password, tenant_name= tennant_name)
        glance_endpoint = keystone.service_catalog.url_for(service_type='image')
        self.__auth = keystone.auth_token
        self.__glance = glclient.Client(glance_endpoint,token=self.__auth)
        if image_name:
            self.__name = image_name
        else:
            self.__name = "Cloudscraper-img-"+str(int(time.clock()))

        images = self.__glance.images.list()
        logging.debug("Connected to glance. Available images:")
        for image in images:
            logging.debug(image.id)

        self.__disk_format = disk_format
        self.__image = None
        self.__diskSize = result_disk_size_bytes
        self.__uploadedSize = 0
        self.__proxyFileObj = None
        self.__container = container_format

        self.__chunkSize = chunksize # 64KB as requested by glance api
        #self.__glance.authenticate()

 
    def initStorage(self):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """
        #TODO: get metadata from the config, or move to instance generator
        #metadata = {'isolate_os':'windows' , 'requires_ssh_key':'false' , 'windows12':'true'}
        # here we should create an image
        self.__image = self.__glance.images.create(name=self.__name, disk_format=self.__disk_format ,container_format=self.__container)

        return True

    def getUploadPath(self):
        """ gets the upload path identifying the upload sufficient to upload the disk in case storage account and container name are already defined"""
        return self.__image.name


    
    def uploadData(self, extent):       
       """Note: should be sequental"""
       
       tarify = False
       if self.__container == "ovf" or self.__container == "ova":
           #NOTE: tarify doesn't work as for now
            tarify = False
           
       if self.__proxyFileObj == None:
           if tarify:
               self.__tar = tarfile.open("cloudscraper"+str(self.__image.id) , mode="w|bz2" , fileobj=self.__proxyFileObj , bufsize=self.__chunkSize)
               r, w = os.pipe() 
               self.__tarProxy = os.fdopen(w, "wb")
               self.__tar.addfile(tarfile.TarInfo("image."+self.__disk_format) , os.fdopen(r, "rb"))
           self.__proxyFileObj = DefferedUploadFile()
           self.__thread = threading.Thread(target = glanceUploadThreadRoutine, args=(self.__glance,self.__proxyFileObj,self.__image,self.__diskSize,tarify,self.__chunkSize) )
           self.__thread.start()
      
       if tarify:
           self.__tarProxy.write(str(extent.getData()))
       else:
           self.__proxyFileObj.write(extent.getData())

       self.__uploadedSize = self.__uploadedSize + extent.getSize()

       return self.__proxyFileObj.cancelled() == False
    

    def waitTillUploadComplete(self):
        self.__proxyFileObj.complete()
        time.sleep(5)
        image = self.__glance.images.get(self.__image.id)
        logging.info("The image upload is complete. Image name: " + str(image.name) + " Id: " + str(image.id) + " Status " + str(image.status))
    
    def confirm(self):
        """
        Confirms good upload
        """
        return self.__image.id

    def getTransferChunkSize(self):
       """
       Gets the size of transfer chunk in bytes.
       All the data except the last chunk should be aligned and be integral of this size    
       """
       return self.__chunkSize

    def getDataTransferRate(self):
       """ 
       Return: 
            float: approx. number of bytes transfered per second 
       """
       return 0

    def getOverallDataSkipped(self):
        """
        Gets overall size of data skipped in bytes. 
        Data is skipped by the channel when the block with same checksum is already present in the cloud
        """
        return 0

    def getOverallDataTransfered(self):
        """
        Gets overall size of data actually uploaded (not skipped) in bytes.
        """
        return self.__uploadedSize

    def close(self):
        """
        Closes the channel, deallocates any associated resources
        """
        return
   

    def getImageSize(self):
        """
        Gets image data size to be uploaded
        """
        return self.__diskSize

    def getDiskUploadedProperty(self):
        """
        Returns amount of data already uploaded as it saved in the cloud storage
        This data could be loaded from the disk object on cloud side which channel represents
        """
        return 0

    def __loadDiskUploadedProperty(self):
        """
        Loads data already uploaded property as it saved in the cloud storage
        Returns False if disk property could be loaded, True if it was loaded and saved, excepts otherwise
        """
        return False