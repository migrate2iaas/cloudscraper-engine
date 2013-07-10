# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import os 
import sys


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

import base64
import math
try:
    from hashlib import md5
except ImportError:
    from md5 import md5


class EHUploadThread(threading.Thread):
    """thread making all uploading works"""
    #skip existing is ignored right now
    def __init__(self , queue , threadId , hostname , ehSession , skipExisting = False , retries = 3):
        self.__uploadQueue = queue
        #thread id , for troubleshooting purposes
        self.__threadId = threadId
        self.__skipExisting = skipExisting
        self.__maxRetries = retries
        self.__EH = ehSession
        self.__hostname = hostname
        return super(EHUploadThread,self).__init__()

    def run(self):
        while 1:

            (driveid , start, size, data_getter) = self.__uploadQueue.get()

            # means it's time to exit
            if driveid == None:
                return
            #NOTE: consider to use multi-upload in case of large chunk sizes
            
            #NOTE: kinda dedup could be added here!
            #NOTE: we should able to change the initial keyname here so the data'll be redirected 

            #TODO: make md5 map for resume uploads

            failed = True
            retries = 0
            while retries < self.__maxRetries:
                retries = retries + 1

                try:
                # s3 key is kinda file in the bucket (directory)
                
                    data = str(data_getter)[0:size]
                    upload = True
                    
                    # we create in-memory gzip file and upload it
                    inmemfile = StringIO.StringIO()
                    gzipfile = gzip.GzipFile("tmpgzip", "wb" , 6 , inmemfile)
                    gzipfile.write(data)
                    gzipfile.close()
                    gzip_data = inmemfile.getvalue()
                    inmemfile.close()
                    
                    if upload:
                        #request = self.__EH.request('POST' ,self.__hostname+"/drives/"+str(driveid)+"/write/"+str(start) , None , gzip_data , {'Content-Type': 'application/octet-stream' ,  'Content-Encoding':'gzip'})
                        #request.hisotry
                        response = self.__EH.post(self.__hostname+"/drives/"+str(driveid)+"/write/"+str(start) , data=gzip_data, headers={'Content-Type': 'application/octet-stream' ,  'Content-Encoding':'gzip' , 'Expect':''})
                    
                    if response.status_code != 204:
                        logging.warning("!Unexpected status code returned by the ElasticHosts write request: " + str(response) + " " + str(response.text))
                        logging.warning("Headers: %s \n Text length= %s gzipped data" , str(response.request.headers) , str(len(response.request.body))  )
                        response.raise_for_status()
                    
                    
                except Exception as e:
                    #reput the task into the queue
                    logging.warning("!Failed to upload data: disk %s at offset %s , making a retry...", str(driveid), str(start) )
                    logging.warning("Exception = " + str(e));
                    continue

                self.__uploadQueue.task_done()
                failed = False
                break

        #TODO: stop the thread, notify the channel somehow
            if failed:
                logging.error("!!! ERROR failed to upload data: disk %s at offser %s, please make a reupload!", str(driveid), str(start) )
                self.__uploadQueue.task_done()



#TODO: inherit from kinda base one
#TODO: descibe the stack backup source->transfer target->media->channel
class EHUploadChannel(UploadChannel.UploadChannel):
    """channel for Elastic Hosts uploading"""

    #TODO: make more reliable statistics

    #TODO: we need kinda open method for the channel
    #TODO: need kinda doc
    #chunk size means one data element to be uploaded. it waits till all the chunk is transfered to the channel than makes an upload (not fully implemented)
    # since there is no one big storage, location is mandatory
    # driveid could be empty if no drive is already created
    # upload threads should be equal to 1 to avoid collisions
    def __init__(self, driveid, userid, apisercret , resultDiskSizeBytes , location , drivename , resumeUpload = False, chunksize = 4096*1024 , uploadThreads=1 , queueSize=16):
        self.__uploadQueue = Queue.Queue(queueSize)
        self.__statLock = threading.Lock()
        self.__prevUploadTime = None 
        self.__transferRate = 0


        #TODO: catch kinda exception here if not connected. shouldn't last too long
        self.__hostname = 'https://api-'+location+'.elastichosts.com'
        self.__EH = requests.Session()
        self.__EH.auth = (userid, apisercret)
        self.__EH.headers.update({'Content-Type': 'text/plain', 'Accept':'application/json'})
        
        self.__region = location

        self.__resumeUpload = resumeUpload

        self.__uploadedSize = 0
        self.__uploadSkippedSize = 0
        self.__overallSize = 0
        self.__chunkSize = chunksize
       
        self.__volumeToAllocateBytes = resultDiskSizeBytes

        #check disk is allocated
        #TODO: move it to kinda method
        # if the drive is already exisits but doesn't fullfill our requirements? 
        # 
        if driveid:
            response = self.__EH.get(self.__hostname+"/drives/" + driveid + "/info")
            if response.status_code != 200:
                logging.warning("!Unexpected status code returned by the ElasticHosts write request: " + str(response) + " " + str(response.text))
                logging.warning("Headers: %s \n Text length= %s gzipped data" , str(response.request.headers) , str(len(response.request.body))  )
                response.raise_for_status()
            self.__driveId = response.json()[u'drive']
            logging.info("\n>>>>>>>>>>> Reupload to ElasticHosts drive "+ str(self.__driveId)+ " !")
            # TODO: test whether the disk created is compatible
        else:
            #NOTE: we skip the resume scenario for now
            createdata = "name "+str(drivename)+"\nsize "+str(self.__volumeToAllocateBytes)
            response = self.__EH.post(self.__hostname+"/drives/create" , data=createdata)
            self.__driveId = response.json()[u'drive']
            logging.info("\n>>>>>>>>>>> New ElasticHosts drive "+ str(self.__driveId)+ " created!")
        
        #dictionary by the start of the block
        self.__fragmentDictionary = dict()
        #initializing a number of threads, they are stopping when see None in queue job
        self.__workThreads = list()
        i = 0
        while i < uploadThreads:
            thread = EHUploadThread(self.__uploadQueue , i ,  self.__hostname , self.__EH , self.__resumeUpload)
            thread.start()
            self.__workThreads.append(thread)
            i = i + 1
        return


    # this one is async
    def uploadData(self, extent):       
       #TODO: monitor the queue sizes
       start = extent.getStart()
       size = extent.getSize()
      
       self.__uploadQueue.put( (self.__driveId, start, size, extent.getData() ) )
       # todo: log
       #TODO: make this tuple more flexible
       self.__fragmentDictionary[start] = (start , size)
       # treating overall size as maximum size
       if self.__overallSize < start + size:
           self.__overallSize = start + size       

       # since there are no data skip and only 1 thread writes in a moment it ok to call it from here
       self.notifyDataTransfered(size)

       return 

    def getUploadPath(self):
        return self.__driveId

   # gets the size of one chunk of data transfered by the each request, the data extent is better to be aligned by the integer of chunk sizes
    def getTransferChunkSize(self):
        return self.__chunkSize

    #returns float: number of bytes transfered in seconds
    def getDataTransferRate(self):
        return self.__transferRate

    #  overall data skipped from uploading if resume upload is set
    def notifyDataSkipped(self , skipped_size):
        with self.__statLock:
            self.__uploadSkippedSize = self.__uploadSkippedSize + skipped_size

    # gets overall data skipped from uploading if resume upload is set
    def getOverallDataSkipped(self):
        return self.__uploadSkippedSize

    def notifyDataTransfered(self , transfered_size):
        now = datetime.datetime.now()
        if self.__prevUploadTime:
            delta = now - self.__prevUploadTime
            # TODO: bad one, should count number of bytes transfered in a minute
            # but still it's kinda approximation (in case one transfer is larger than one second)
            if delta.seconds:
                self.__transferRate = transfered_size/delta.seconds
        self.__prevUploadTime = now
        with self.__statLock:
            self.__uploadedSize = self.__uploadedSize + transfered_size

    #gets the overall size of data uploaded
    def getOverallDataTransfered(self):
        return self.__uploadedSize 

    # wait uploaded all needed
    def waitTillUploadComplete(self):
        self.__uploadQueue.join()
        return

    # confirm good upload. uploads resulting xml then, returns the id of the upload done
    def confirm(self):
        #TODO: here we may generate kinda crc32 map for faster uploading
        return self.__driveId

    def close(self):
        for thread in self.__workThreads:
            self.__uploadQueue.put( (None , None, None, None ) )
   

        