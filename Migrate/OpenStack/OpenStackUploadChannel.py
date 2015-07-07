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


def glanceUploadThreadRoutine(glance, proxyFileObj, image,size):
    try:
        logging.info(">>> Image transfer being");
        glance.images.upload(image.id,proxyFileObj)
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

    def __init__(self, result_disk_size_bytes, server_url , tennant_name , username , password, disk_format = "vhd", image_name=None, resume_upload = False , chunksize=64*1024 , upload_threads=1 , queue_size=1):
        """constructor"""
        keystone = ksclient.Client(auth_url=server_url,   username=username, password=password, tenant_name= tennant_name)
        glance_endpoint = keystone.service_catalog.url_for(service_type='image')
        self.__auth = keystone.auth_token
        self.__glance = glclient.Client(glance_endpoint,token=self.__auth)
        if image_name:
            self.__name = image_name
        else:
            self.__name = "Cloudscraper-img-"+str(int(time.clock()))
        self.__disk_format = disk_format
        self.__image = None
        self.__diskSize = result_disk_size_bytes
        self.__uploadedSize = 0
        self.__proxyFileObj = None
        #self.__glance.authenticate()

 
    def initStorage(self):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """

        # here we should create an image
        self.__image = self.__glance.images.create(name=self.__name, disk_format=self.__disk_format ,container_format="bare")

        return True

    def getUploadPath(self):
        """ gets the upload path identifying the upload sufficient to upload the disk in case storage account and container name are already defined"""
        return self.__image.name


    
    def uploadData(self, extent):       
       """Note: should be sequental"""
       
       if self.__proxyFileObj == None:
           self.__proxyFileObj = DefferedUploadFile()
           self.__thread = threading.Thread(target = glanceUploadThreadRoutine, args=(self.__glance,self.__proxyFileObj,self.__image,self.__diskSize) )
           self.__thread.start()
       
       self.__proxyFileObj.write(extent.getData())

       self.__uploadedSize = self.__uploadedSize + extent.getSize()

       return self.__proxyFileObj.cancelled() == False
    

    def waitTillUploadComplete(self):
        self.__proxyFileObj.complete()
    
    def confirm(self):
        """
        Confirms good upload
        """
        return self.__image.id

   