"""
This file defines upload to OpenStack via swift
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
import threading



import zlib
import gzip
import StringIO
import UploadChannel
import MultithreadUpoadChannel
from md5 import md5


from DefferedUploadFile import DefferedUploadFile
import threading
import __builtin__
import time
import os.path



class DefferedMultifileUploadFileProxy(object):
    def __init__(self , deffered_data):
        self.__pos = 0
        self.__data = deffered_data

    def read(self , len):
        data = self.__data.readData(len , self.__pos)
        self.__pos += len
        return data
    
    def write(self, buf):
        raise NotImplementedError

    def close(self):
        pass

    def seek(self, pos, whence=0):
        self.__pos = pos


class DefferedUploadDataStream(object):

    opened_streams = dict()
    @staticmethod
    def getFileProxy(name):
        return DefferedMultifileUploadFileProxy(DefferedUploadDataStream.opened_streams[name])

    @staticmethod
    def getStream(name):
        return DefferedUploadDataStream.opened_streams[name]

    def __init__(self , name , size, chunksize, max_part_number=100):
        self.__pos = 0
        self.__size = size
        self.__parts = dict()
        self.__chunksize = chunksize
        DefferedUploadDataStream.opened_streams[name] = self
        self.__semaphore = threading.Semaphore(max_part_number)
        self.__dictLock = threading.Lock()
        self.__cancel = False

    def readData(self , len , pos):
        while 1:
            interval_start = self.__chunksize * int(pos / self.__chunksize)
            with self.__dictLock:
                if self.__parts.has_key(interval_start):
                    #logging.debug("Getting data at pos " + str(pos) + " of len " + str(len) )
                    data = self.__parts[interval_start][pos-interval_start:pos-interval_start+len]
                    if pos-interval_start+len == self.__chunksize:
                        del self.__parts[interval_start]
                        self.__semaphore.release()
                        logging.debug("Removed entry from deffered upload stream at pos " + str(interval_start) )
                    if data == None:
                        logging.warning("!No data available")
                        data = ""
                    return data
            # not sure if it works. Usually sleep is the worst method to do multithreading
            # we just hope network calls for data not so often
            time.sleep(1)
            logging.debug("Waiting till data is available at pos " + str(pos))

    def writeData(self , buf , pos):
        logging.debug("Adding more data to deffered upload stream at pos " + str(pos))
        self.__semaphore.acquire()
        logging.debug("Added more data to deffered upload stream at pos " + str(pos))
        with self.__dictLock:
            self.__parts[pos] = buf

    def seek(self, pos):
        self.__pos = pos

    def getSize(self):
        return self.__size

    def cancel(self):
        self.__cancel = True

    def cancelled(self):
        return self.__cancel

# a hack to open different file object
def defferedOpen(path , mode=None):
    if "deffered://" in path:
        return DefferedUploadDataStream.getFileProxy(path)
    return original_open(path,mode)

import swiftclient
from swiftclient.service import SwiftService, SwiftError, SwiftUploadObject
from swiftclient.utils import generate_temp_url

def getSize(path):
    if "deffered://" in path:
        return DefferedUploadDataStream.getStream(path).getSize()
    return original_getsize(path)

def defferedStat(path):
    if "deffered://" in path:
        # return some fake stat
        return original_stat(__file__)
    return original_stat(path)

# set original funcs globals
if not swiftclient.service.__builtins__['open'] == defferedOpen:
    original_open = __builtin__.open
    
if not os.path.getsize == getSize:
    original_getsize = os.path.getsize

if not swiftclient.service.stat == defferedStat:
    original_stat = swiftclient.service.stat


def swiftUploadThreadRoutine(proxyFileObj, container, upload_object, swiftService):
    try:
        verbose = True
        proxy_file = proxyFileObj
        logging.info(">>> Image transfer begins");
        options = {'segment_container':container , 'use_slo':True}
        results = swiftService.upload(container , [upload_object], options=options)
        for r in results:
            if r['success']:
                if verbose:
                    if 'attempts' in r and r['attempts'] > 1:
                        if 'object' in r:
                            logging.debug(
                                '%s [after %d attempts]' %
                                (r['object'],
                                    r['attempts'])
                            )
                    else:
                        if 'object' in r:
                            logging.debug(r['object'])
                        elif 'for_object' in r:
                            logging.debug(
                                '%s segment %s' % (r['for_object'],
                                                    r['segment_index'])
                            )
            else:
                error = r['error']
                logging.error("!!!ERROR: while uploading %s" % error)
                too_large = (isinstance(error, swiftclient.exceptions.ClientException) and
                                error.http_status == 413)
                if too_large and options.verbose > 0:
                    logging.error(
                            "Consider using the --segment-size option "
                            "to chunk the object")
                proxyFileObj.cancel()
                return

        logging.info(">>> Image transfer complete");
    except Exception as e:
         logging.warning("!Failed to upload data")
         logging.warning("Exception = " + str(e)) 
         logging.error(traceback.format_exc())
         proxyFileObj.cancel()

class SwiftUploadChannel(UploadChannel.UploadChannel):
    """
    Upload channel for OpenStack swift
    Implements multithreaded multipart file upload to OpenStack swift
    """

    def __init__(self ,resulting_size_bytes , server_url, username, tennant_name , password , disk_name, container_name , compression = False \
                 , resume_upload = False , chunksize=10*1024*1024 , upload_threads=10):
        """constructor"""
        self.__chunkSize = chunksize
        self.__accountName = username
        self.__containerName = container_name
        self.__diskName = disk_name
        self.__accessKey = password
        self.__resumeUpload = resume_upload
        self.__uploadThreads = upload_threads
        self.__serverUrl = server_url
        self.__diskSize = resulting_size_bytes

        self.__nullData = bytearray(self.__chunkSize)
        md5encoder = md5()
        md5encoder.update(self.__nullData)
        self.__nullMd5 = md5encoder.hexdigest()
        self.__sslCompression = compression

        self.__proxyFileObj  = None
        self.__uploadedSize = 0

        self.__serviceOpts = { 'auth' : server_url , 'user':tennant_name+":"+self.__accountName , "key":self.__accessKey , "auth_version":"2" , \
            "segment_threads":upload_threads , "segment_size" : chunksize , \
           'ssl_compression' : self.__sslCompression }
        self.__swiftService = SwiftService(options = self.__serviceOpts)

 
    def initStorage(self, init_data_link=""):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """
        #we make it public for now
        options = {'read_acl':'.r:*'}
        result = self.__swiftService.post(self.__containerName , options=options)
        if not result["success"]:
            logging.error("!!!ERROR: Couldn't connect to swift: " + repr(result))
            return False
        return True

    def uploadData(self, extent):       
       """Note: should be sequental"""
       
       #TODO: rebuild segment upload routines to support reupload. Consider using static large objects - their logic seems simpler to follow
       # see http://docs.openstack.org/developer/swift/middleware.html#slo-doc
           
       # we amke a deffered file writer to emulate data streaming to swift. dirty hack but fastest way to reuse openstack streaming routines
       if self.__proxyFileObj == None:

           # set some hooks\hacks to make swift lib work with a stream-like object instead of external file
           if not swiftclient.service.__builtins__['open'] == defferedOpen:
               original_open = __builtin__.open
               swiftclient.service.__builtins__['open'] = defferedOpen
    
           if not os.path.getsize == getSize:
                original_getsize = os.path.getsize
                swiftclient.service.getsize = getSize

           if not swiftclient.service.stat == defferedStat:
                original_stat = swiftclient.service.stat
                swiftclient.service.stat = defferedStat
                os.stat = defferedStat

           deffered_path = "deffered://"+self.__diskName
           self.__proxyFileObj = DefferedUploadDataStream(deffered_path , self.__diskSize, self.__chunkSize, self.__uploadThreads*2)
           upload_object = SwiftUploadObject(deffered_path,self.__diskName)
           self.__thread = threading.Thread(target = swiftUploadThreadRoutine, args=(self.__proxyFileObj,self.__containerName,upload_object, self.__swiftService) )
           self.__thread.start()
      
       self.__proxyFileObj.writeData(extent.getData() , extent.getStart())

       self.__uploadedSize = self.__uploadedSize + extent.getSize()

       if self.__proxyFileObj.cancelled():
           logging.error("Upload process is cancelled due to failures")

       return self.__proxyFileObj.cancelled() == False
    

    def waitTillUploadComplete(self):
        self.__thread.join()
        time.sleep(5)
        logging.info("The image upload to Swift storage is complete.")
    
    def getUploadPath(self):
        """ gets the upload path identifying the upload: object name """
        return self.__diskName

    def getUploadedUrl(self):
        """gets the temp url for the uploaded disk"""
        stat_options = {'verbose':2}
        account_stat = self.__swiftService.stat(options=stat_options)
        logging.debug("Account stat: " + repr(account_stat))
        storage_url = ""
        for stat_tuples in account_stat['items']:
            if str(stat_tuples[0]) == "StorageURL":
                storage_url = stat_tuples[1]
                break

        if not storage_url:
            logging.error("!!!ERROR: cannot find storage account url!")
            return None

        if account_stat['headers'].has_key('X-Account-Meta-Temp-URL-Key'):
            key = account_stat['headers']['X-Account-Meta-Temp-URL-Key']
        elif account_stat['headers'].has_key('X-Account-Meta-Temp-URL-Key'.lower()):
            key = account_stat['headers']['X-Account-Meta-Temp-URL-Key'.lower()]
        else:
            logging.info("Account URL Key not found, setting our own one")
            key = self.__accessKey
            options = {'headers':{'':key}}
            self.__swiftService.post(options = options)

        logging.info(repr(account_stat))
        path_url_part = storage_url[storage_url.find("/v1/"):]
        base_url_part = storage_url.replace(path_url_part , "")
        path = path_url_part+"/"+self.__containerName+"/"+self.__diskName
        method = "GET"
        
        seconds =  24*60*60 # 24 hours
        # dunno but Webzilla takes milliseconds instead of seconds
        milliseconds = int(time.time())*1000 + seconds*1000 - time.time() + seconds
        #milliseconds = 1440535811144 - time.time()
        #key should be set as metdata Temp-URL-Key:
        url = generate_temp_url(path, milliseconds, key, method)
        logging.debug("Swift temp url is " + url)
        # temp url doesn't work if object is not public - glance ignores the URI signature and parms when making request
        return base_url_part+path #+url

    def confirm(self):
        """
        Confirms good upload
        """
        #Roll back the hacks
        if swiftclient.service.__builtins__['open'] == defferedOpen:
           swiftclient.service.__builtins__['open'] = original_open
    
        if os.path.getsize == getSize:
           os.path.getsize = original_getsize 
           swiftclient.service.getsize = original_getsize

        if swiftclient.service.stat == defferedStat:
           swiftclient.service.stat = original_stat
           os.stat = original_stat

        url = self.getUploadedUrl()

        return url

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