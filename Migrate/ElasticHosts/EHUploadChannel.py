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
from md5 import md5


UPLOADED_BEFORE_JSON_KEY = u'user:uploaded'

# NOTE: only one thread does really work. EH doesn't support concurent access
class EHUploadThread(threading.Thread):
    """thread making all uploading works"""
    #skip existing is ignored right now
    def __init__(self , queue , threadId , hostname , ehSession , skipExisting = False , channel = None , retries = 3 , compression = 3):
        self.__uploadQueue = queue
        #thread id , for troubleshooting purposes
        self.__threadId = threadId
        self.__skipExisting = skipExisting
        self.__maxRetries = retries
        self.__EH = ehSession
        self.__hostname = hostname
        self.__channel = channel
        self.__compression = compression
        super(EHUploadThread, self).__init__()

    def run(self):
        iterations = 0
        update_size_every = 32
        while 1:
            iterations = iterations + 1
            (driveid , start, size, data_getter) = self.__uploadQueue.get()

            # NOTE: its ok since we have only one thread
            if iterations % update_size_every == 0:
                self.__channel.updateDiskUploadedProperty()
            # means it's time to exit
            if driveid == None:
                self.__channel.updateDiskUploadedProperty()
                return
            #NOTE: consider to use multi-upload in case of large chunk sizes
            
            #NOTE: kinda dedup could be added here!
            #NOTE: we should able to change the initial keyname here so the data'll be redirected 
            failed = True
            retries = 0
            while retries < self.__maxRetries:
                retries = retries + 1

                try:
                
                    data = str(data_getter)[0:size]
                    upload = True

                    if data.count("\x00") == size:
                        logging.debug("Skipping the 0-block at offset " + str(start));
                        upload = False

                    # check if 
                    if self.__skipExisting:
                        already_uploaded = self.__channel.getDiskUploadedProperty()
                        # seems like python has troubels comparing longs and ints
                        if long(already_uploaded) >= long(start + size):
                            upload = False
                            logging.debug("Skipped the uploading of data at offset " + str(start) + " because " + str(already_uploaded) + "bytes were already uploaded")

                    if upload:
                        # we create in-memory gzip file and upload it
                        inmemfile = StringIO.StringIO()
                        gzipfile = gzip.GzipFile("tmpgzip", "wb" , self.__compression , inmemfile)
                        gzipfile.write(data)
                        gzipfile.close()
                        gzip_data = inmemfile.getvalue()
                        inmemfile.close()

                        response = self.__EH.post(self.__hostname+"/drives/"+str(driveid)+"/write/"+str(start) , \
                            data=gzip_data, headers={'Content-Type': 'application/octet-stream' ,  'Content-Encoding':'gzip' , 'Expect':''})
                        if response.status_code != 204:
                            logging.warning("!Unexpected status code returned by the ElasticHosts write request: " + str(response) + " " + str(response.text))
                            logging.warning("Headers: %s \n Text length= %s gzipped data" , str(response.request.headers) , str(len(response.request.body))  )
                            response.raise_for_status()
                        else:
                            self.__channel.notifyDataTransfered(size)
                    else:
                        self.__channel.notifyDataSkipped(size)
                  
                    
                except Exception as e:
                    #reput the task into the queue
                    logging.warning("!Failed to upload data: disk %s at offset %s , making a retry...", str(driveid), str(start) )
                    logging.warning("Exception = " + str(e)) 
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
    def __init__(self, driveid, userid, apisercret , resultDiskSizeBytes , location , drivename , cloudoptions , resumeUpload = False, uploadThreads=1 , queueSize=1):
        self.__uploadQueue = Queue.Queue(queueSize)
        self.__statLock = threading.Lock()
        self.__prevUploadTime = None 
        self.__transferRate = 0
        
        chunksize = cloudoptions.getUploadChunkSize()
        avoid = cloudoptions.getZone()

        # We make it possible to use other ElasticStack clouds given by a direct link
        if location.find("https://") != -1 or location.find("http://") != -1:
            self.__hostname = location
        else:
            self.__hostname = 'https://api-'+location+'.elastichosts.com'

        self.__EH = requests.Session()
        self.__EH.auth = (userid, apisercret)
        self.__EH.headers.update({'Content-Type': 'text/plain', 'Accept':'application/json'})
        
        self.__region = location
        self.__driveName = drivename
        self.__resumeUpload = resumeUpload

        self.__uploadedSize = 0
        self.__uploadSkippedSize = 0
        self.__overallSize = 0
        self.__chunkSize = chunksize
        self.__alreadyUploaded = 0
       
        self.__volumeToAllocateBytes = resultDiskSizeBytes
        self.__allocatedDriveSize = resultDiskSizeBytes

        #check disk is allocated
        #TODO: move it to kinda method
        # if the drive is already exisits but doesn't fullfill our requirements? 
        # 
        if driveid:
            response = self.__EH.get(self.__hostname+"/drives/" + driveid + "/info")
            if response.status_code != 200:
                logging.warning("!Unexpected status code returned by the ElasticHosts request: " + str(response) + " " + str(response.text))
                logging.warning("Headers: %s \n" , str(response.request.headers) )
                response.raise_for_status()
            self.__driveId = response.json()[u'drive']
            self.__allocatedDriveSize = response.json()[u'size']
            uploadedsize = response.json().get(UPLOADED_BEFORE_JSON_KEY)
            if uploadedsize:
                self.__alreadyUploaded = uploadedsize
            if resultDiskSizeBytes > self.__allocatedDriveSize:
                logging.error("\n!!!ERROR: The disk " + str(self.__driveId) + " size is not sufficient to store an image!") 
                raise IOError
            logging.info("\n>>>>>>>>>>> Reupload to ElasticHosts drive "+ str(self.__driveId)+ " !")
            logging.debug(str(self.__alreadyUploaded) + " bytes were already uploaded to the cloud")
            # TODO: test whether the disk created is compatible
        else:
            createdata = "name "+str(self.__driveName)+"\nsize "+str(self.__volumeToAllocateBytes)
            if avoid:
                createdata = createdata + "\navoid " + avoid
            response = self.__EH.post(self.__hostname+"/drives/create" , data=createdata)
            if response.status_code != 200:
                logging.warning("!Unexpected status code returned by the ElasticHosts request: " + str(response) + " " + str(response.text))
                logging.error("\n!!!ERROR: Cannot create new disk of "+ str(self.__allocatedDriveSize) + " bytes at ElasticHosts");
                logging.warning("Headers: %s \n" , str(response.request.headers) )
                logging.warning(">>>>>>>>>>>>>> Note: ElasticHosts trial account provides 20GB of data storage only. Please contact support@elastichosts.com to extend your storage size.")
                response.raise_for_status()
            self.__driveId = response.json()[u'drive']
            self.__allocatedDriveSize = response.json()[u'size']
            logging.info("\n>>>>>>>>>>> New ElasticHosts drive "+str(drivename)+ " UUID: " + str(self.__driveId)+ " created!")
            logging.info("Drive size = " + str(self.__allocatedDriveSize)) 
        
        #dictionary by the start of the block
        self.__fragmentDictionary = dict()
        #initializing a number of threads, they are stopping when see None in queue job
        self.__workThreads = list()
        i = 0
        while i < uploadThreads:
            thread = EHUploadThread(self.__uploadQueue , i ,  self.__hostname , self.__EH , self.__resumeUpload , self)
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

    # The assumption is data is uploaded lineary. 
    # it's true due to use of one uploading thread 
    # TODO: find a better way!
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
            self.__uploadedSize = self.__uploadedSize + int(transfered_size)
            self.__alreadyUploaded = self.__uploadSkippedSize + self.__uploadedSize

    #gets the overall size of data uploaded
    def getOverallDataTransfered(self):
        return self.__uploadedSize 

    # wait uploaded all needed
    def waitTillUploadComplete(self):
        self.__uploadQueue.join()
        return

    # confirm good upload. uploads resulting xml then, returns the id of the upload done
    def confirm(self):
        self.updateDiskUploadedProperty()
        #TODO: here we may generate kinda crc32 map for faster uploading
        return self.__driveId

    def close(self):
        for thread in self.__workThreads:
            self.__uploadQueue.put( (None , None, None, None ) )
   
    # function to notify EH on the data amount transfered
    def updateDiskUploadedProperty(self):
        setinfodata = "name "+str(self.__driveName) + "\nsize "+str(self.__allocatedDriveSize) + "\n"+str(UPLOADED_BEFORE_JSON_KEY) +" "+str(self.__alreadyUploaded)
        response = self.__EH.post(self.__hostname+"/drives/"+self.__driveId+"/set" , data=setinfodata)
        if response.status_code != 200 and response.status_code != 204:
            logging.warning("!Unexpected status code returned by the ElasticHosts set disk info request: " + str(response) + " " + str(response.text))
            logging.warning("Headers: %s \n Text length= %s , data = %s" , str(response.request.headers) , str(len(response.request.body)) ,  str(response.request.body))
            response.raise_for_status()
    
    def getDiskUploadedProperty(self):
        return self.__alreadyUploaded