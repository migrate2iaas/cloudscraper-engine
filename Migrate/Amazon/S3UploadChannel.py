# --------------------------------------------------------
import hashlib
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import os 
import sys


import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from boto.exception import BotoServerError

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
import traceback

import zlib
import gzip
import StringIO

import logging
import threading 
import datetime
import UploadChannel



import base64
import math
from md5 import md5

class S3ManfiestBuilder:

    def __init__(self , tmpFileName, s3XmlKey , bucketname , s3connection,  fileFormat = 'VHD'):
        self.__file = open(tmpFileName , "wb")
        self.__xmlKey = s3XmlKey
        self.__fileFormat = fileFormat
        self.__bucket = bucketname
        self.__S3 = s3connection
        return

    def buildHeader(self , bytesToUpload , resultingSizeGb , fragmentCount):
      #TODO: change file format
       header =  '<?xml version="1.0" encoding="UTF-8" standalone="yes"?> \n\t <manifest> \n\t\t <version>2010-11-15</version> \n\t\t <file-format>'+self.__fileFormat+'</file-format> \n\t\t <importer>'
       self.__file.write(header)

       #TODO: we emulate the standart utility XML (otherwise, the import won't work properly)
       importerver = '\n\t\t\t <name>ec2-upload-disk-image</name> \n\t\t\t <version>1.0.0</version> \n\t\t\t  <release>2010-11-15</release>'
       self.__file.write(importerver)
       
       linktimeexp_seconds = 60*60*24*15 # 15 days
       urldelete = self.__S3.generate_url( linktimeexp_seconds, method='DELETE', bucket=self.__bucket,  key=self.__xmlKey, force_http=False)
       selfdestruct = '\n\t\t </importer> \n\t\t  <self-destruct-url>'+urldelete.replace('&' ,'&amp;')+'</self-destruct-url> '
       self.__file.write(selfdestruct)

       importvol = '\n\t\t <import> \n\t\t\t <size>'+ str(bytesToUpload) + '</size> \n\t\t\t <volume-size>' + str(resultingSizeGb) + '</volume-size>' + '\n\t\t\t <parts count="'+str(fragmentCount)+'">'
       self.__file.write(importvol)
         
       return

    def addUploadedPart(self , index , rangeStart , rangeEnd , keyName):
        
        linktimeexp_seconds = 60*60*24*15 # 15 days

        indexstr = '\n\t\t\t <part index="'+str(index)+'">\n\t\t\t\t '+'<byte-range end="' + str (rangeEnd) + '" start="'+ str(rangeStart) +'" />'
        self.__file.write(indexstr)

        keystr = '\n\t\t\t\t <key>'+str(keyName)+'</key>'
        self.__file.write(keystr)
          
        urlhead = self.__S3.generate_url( linktimeexp_seconds, method='HEAD', bucket=self.__bucket, key=keyName, force_http=False)
        gethead = '\n\t\t\t\t <head-url>'+urlhead.replace('&' ,'&amp;')+'</head-url>'
        self.__file.write(gethead)

        urlget = self.__S3.generate_url( linktimeexp_seconds, method='GET', bucket=self.__bucket, key=keyName, force_http=False)
        getstr = '\n\t\t\t\t <get-url>'+urlget.replace('&' ,'&amp;')+'</get-url>'
        self.__file.write(getstr)

        urldelete = self.__S3.generate_url( linktimeexp_seconds, method='DELETE', bucket=self.__bucket, key=keyName, force_http=False)
        getdelete = '\n\t\t\t\t <delete-url>'+urldelete.replace('&' ,'&amp;')+'</delete-url>'
        self.__file.write(getdelete)

        partend = '\n\t\t\t </part>'
        self.__file.write(partend)

        return

    def finalize(self):
        end = '\n </parts>\n </import> \n </manifest>\n'
        self.__file.write(end)
        self.__file.close()
        return 

#TODO: add wait completion\notification\delegates on this part completion
class UploadQueueTask(object):
    # NOTE: make kinda abstraction for alternative sources. Still more design effort is needed to get what these source\buckets should really be
    def __init__(self, bucket , keyname, size, data_getter, channel , alternative_source_bucket = None, alternative_source_keyname = None):
        self.__channel = channel
        self.__targetBucket = bucket
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

    def getTargetBucket(self):
        return self.__targetBucket

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

    def getAlternativeKeyName(self):
        return self.__alternativeKey

    def getAlternativeBucket(self):
        return self.__alternativeBucket

    def getAlternativeKey(self):
        """gets alternative key object"""
        return self.__targetBucket.get_key(self.__alternativeKey)


class S3UploadThread(threading.Thread):
    """thread making all uploading works"""
    def __init__(self , queue , threadid , skipexisting = False , channel = None , copysimilar = True,  retries = 3):
        self.__uploadQueue = queue
        #thread id , for troubleshooting purposes
        self.__threadId = threadid
        self.__skipExisting = skipexisting
        self.__maxRetries = retries
        self.__copySimilar = copysimilar
        super(S3UploadThread,self).__init__()

    def run(self):
        while 1:

            # TODO: make a better tuple
            # how-to get the data. 
            # it could be got for example from other source.
            # data is how it could be read from source (by the target) 
            # and could be written (to the target)
            uploadtask = self.__uploadQueue.get()
            
            # means it's time to exit
            if uploadtask == None:
                return

            bucket = uploadtask.getTargetBucket()
            keyname = uploadtask.getTargetKey()
            size = uploadtask.getSize()
            data = uploadtask.getData()
            md5_hexdigest = uploadtask.getDataMd5()

            # means it's time to exit
            if bucket == None:
                return
            
            #NOTE: kinda dedup could be added here!
            
            failed = True
            retries = 0
            while retries < self.__maxRetries:
                retries = retries + 1

                try:
                # s3 key is kinda file in the bucket (directory)           
                    upload = True
                    s3key = bucket.get_key(keyname)
                
                    # Note: it seems there should be a better (more generic and extendable way) to implement strategies to reduce the overall upload size

                    if s3key == None:
                        if self.__copySimilar and uploadtask.isAlternitiveAvailable():
                            source3key = uploadtask.getAlternativeKey()
                            # extra check. could be avoided
                            existing_md5 = source3key.etag
                            if '"'+str(md5_hexdigest)+'"' == existing_md5:
                                 logging.debug("key with same md5 and length already exisits, skip uploading") 
                                 upload = False
                            s3key = source3key.copy(bucket.name , keyname)
                            upload = False
                        else:
                            s3key = Key(bucket , keyname)    
                    else: 
                        if self.__skipExisting:
                            # check the size and checksums in order to skip upload
                            #TODO: metadata is empty! should recheck the upload
                            # TODO: make metadata upload!
                            existing_length = s3key.size 
                            if int(existing_length) == size:
                                existing_md5 = s3key.etag
                                #md5digest, base64md5 = s3key.get_md5_from_hexdigest(md5_hexdigest) 
                                if '"'+str(md5_hexdigest)+'"' == existing_md5:
                                    logging.debug("key with same md5 and length already exisits, skip uploading") 
                                    upload = False

                    if upload:
                        md5digest, base64md5 = s3key.get_md5_from_hexdigest(md5_hexdigest) 
                        s3key.set_contents_from_string(data, replace=True, policy=None, md5=(md5digest, base64md5), reduced_redundancy=False, encrypt_key=False)
                        uploadtask.notifyDataTransfered()
                    else:
                        logging.debug("Skipped the data upload: %s/%s " , str(bucket), keyname) 
                        uploadtask.notifyDataSkipped()
                    s3key.close()
                except Exception as e:
                    # reput the task into the queue
                    logging.warning("!Failed to upload data: %s/%s , making a retry...", str(bucket), keyname )
                    logging.warning("Exception = " + str(e)) 
                    logging.error(traceback.format_exc()) 
                    continue

                logging.debug("Upload thread "+str(self.__threadId)+ " set queue task done");
                self.__uploadQueue.task_done()
                failed = False
                break

            #TODO: stop the thread, notify the channel somehow
            if failed:
                logging.error("!!! ERROR failed to upload data: %s/%s!", str(bucket), keyname )
                self.__uploadQueue.task_done()
                uploadtask.notifyDataTransferError()
            


#TODO: inherit from kinda base one
#TODO: descibe the stack backup source->transfer target->media->channel
class S3UploadChannel(UploadChannel.UploadChannel):
    """channel for s3 uploading"""

    #TODO: make more reliable statistics

    #TODO: we need kinda open method for the channel
    #TODO: need kinda doc
    #chunk size means one data element to be uploaded. it waits till all the chunk is transfered to the channel than makes an upload (not fully implemented)
    def __init__(self, bucket, awskey, awssercret , resultDiskSizeBytes , location = '' , keynameBase = None, diskType = 'VHD' , resume_upload = False , chunksize=10*1024*1024 , upload_threads=4 , queue_size=16):
        self.__uploadQueue = Queue.Queue(queue_size)
        self.__statLock = threading.Lock()
        self.__prevUploadTime = None 
        self.__transferRate = 0

        #TODO:need to save it in common log directory
        boto.set_file_logger("boto", "boto.log", level=logging.DEBUG)

        awsregion = location
        if location == 'us-east-1':
           awsregion = ''  

        offline = False # to do some offline testing
        #TODO: catch kinda exception here if not connected. shouldn't last too long
        if offline == False:
            hostname = 's3.amazonaws.com'
            if awsregion:
                hostname = 's3-'+awsregion+'.amazonaws.com'
            self.__S3 = S3Connection(awskey, awssercret, is_secure=True, host=hostname)
        
            self.__bucketName = bucket
            try:
                self.__bucket = self.__S3.get_bucket(self.__bucketName)
            except Exception as ex:
                logging.info(">>>>> Creating a new S3 bucket: " + self.__bucketName) 
                try:
                    self.__bucket = self.__S3.create_bucket(self.__bucketName , location=awsregion)
                except BotoServerError as botoex:
                    logging.error("!!!ERROR: Wasn't able to find or create bucket " + self.__bucketName + " in region " + location + " .")
                    logging.error("!!!ERROR: " + botoex.error_message) 
                    logging.error(traceback.format_exc()) 
                    raise botoex
                except Exception as ex:
                    logging.error("!!!ERROR: Wasn't able to find or create bucket " + self.__bucketName + " in region " + location + " .")
                    logging.error("!!!ERROR: " + str(ex)) 
                    logging.error("!!!ERROR: It's possible the bucket with the same name exists but in another region. Try to specify another bucket name for the upload")
                    logging.error(traceback.format_exc()) 
                    raise ex
    

        self.__chunkSize = chunksize 
        self.__region = awsregion

        self.__diskType = diskType
        self.__resumeUpload = resume_upload
        self.__errorUploading = False

        self.__uploadedSize = 0
        self.__uploadSkippedSize = 0
        self.__xmlKey = None
        self.__overallSize = 0
       
        gigabyte = 1024*1024*1024
        self.__volumeToAllocateGb = int((resultDiskSizeBytes+gigabyte-1)/gigabyte)
        
        now = time.localtime()

        if keynameBase:
            self.__keyBase = keynameBase
        else:
            # migrate + number of seconds since 1980
            self.__keyBase = "Migrate" +str(long(time.mktime(now))) + "/image"

        logging.info("\n>>>>>>>>>>>>>>>>> Initializing cloud storage\n") 
        # creating null block to optimize downloads
        self.__nullData = bytearray(self.__chunkSize)
        md5encoder = md5()
        md5encoder.update(self.__nullData)
        self.__nullMd5 = md5encoder.hexdigest()
        self.__nullKey = self.__keyBase + "NullBlock"
        try:
            nullkey = Key(self.__bucket , self.__nullKey)    
            nullkey.set_contents_from_string(self.__nullData, replace=False, policy=None)
        except Exception as ex:
            logging.error("!!!ERROR: Failed to make a test upload to bucket " + self.__bucketName)
            logging.error("Exception = " + str(ex)) 
            logging.error(traceback.format_exc()) 
            raise ex

        #dictionary by the start of the block
        self.__fragmentDictionary = dict()
        #initializing a number of threads, they are stopping when see None in queue job
        self.__workThreads = list()
        i = 0
        while i < upload_threads:
            thread = S3UploadThread(self.__uploadQueue , i , self.__resumeUpload , self)
            thread.start()
            self.__workThreads.append(thread )
            i = i + 1
        return

        logging.info("Succesfully created an upload channel to S3 bucket " + self.__bucketName  + " at " +  location)

    def getUploadPath(self):
        """ gets the upload path identifying the upload: bucket/key """
        return self.__bucketName + "/" +self.__keyBase

    # this one is async
    def uploadData(self, extent):       
       """ 
       Uploads data extent

       See UploadChannel.UploadChannel for more info
       """
       #TODO: monitor the queue sizes
       start = extent.getStart()
       size = extent.getSize()
       keyname =  self.__keyBase+".part"+str(int(start/self.__chunkSize))
       
       #NOTE: the last one could be less than 10Mb.
       # there are two options 1) to align the whole file or to make it possible to use the smaller chunks

       #TODO: test the resume upload scenario on these failed tasks
       #should split the chunk or wait till new data arrives for the same data block
       # the last one could be less than 10Mb
       if size != self.__chunkSize:
           logging.warning("Bad chunk size for upload , should be " + str(self.__chunkSize) ) 

       
       uploadtask = UploadQueueTask(self.__bucket , keyname, size, extent.getData() , self )
       if uploadtask.getDataMd5() == self.__nullMd5:
           if uploadtask.getData() == self.__nullData:
               if self.__nullKey:
                    uploadtask.setAlternativeUploadPath(self.__bucket , self.__nullKey)
               

       #TODO: check threads are working ok
       self.__uploadQueue.put( uploadtask )
       # todo: log
       #TODO: make this tuple more flexible
       #TODO: make kinda fragment database with uploaded flag, md5, etc
       self.__fragmentDictionary[start] = (keyname , size)
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

        # the default value. means lots of time
        linktimeexp_seconds = 1361188806

        #TODO: profile
        starts = self.__fragmentDictionary.keys()
        starts.sort()
        fragment_index = 0
        fragment_count = len(starts)

        
        # NOTE: we catch the security warning from tempnam (really, there is no unsecure data, not sure however)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            xmltempfile = os.tempnam("s3manifest") + ".xml"

        #keyprefix = "result"
        #NOTE: really we may not build XML, it'd be built by ec2-import-script
        #but the parts should be named in a right way
        xmlkey = self.__keyBase+"manifest.xml"

        manifest = S3ManfiestBuilder( xmltempfile , xmlkey , self.__bucketName, self.__S3, self.__diskType)	
        manifest.buildHeader(self.__overallSize , self.__volumeToAllocateGb , fragment_count)
        

        #TODO: faster the solution by re-sorting the dictionary or have an another container
        for start in starts:
            (keyname , size) = self.__fragmentDictionary[start]
            
            # here we rename the parts to reflect their sequence numbers
            # NOTE: it takes to much time. 
            #newkeyname = self.__keyBase+"/"+keyprefix+".part"+str(fragment_index);
            #self.__bucket.copy_key(newkeyname, self.__bucketName, keyname)
            #self.__bucket.delete_key(keyname)

            manifest.addUploadedPart(fragment_index, start , start+size , keyname)
            fragment_index = fragment_index + 1
        
        manifest.finalize()

               
        s3key = Key(self.__bucket , xmlkey)
        s3key.set_contents_from_filename(xmltempfile) 
        s3key.close()
        
        self.__xmlKey = 'https://s3.amazonaws.com/' + str(self.__bucketName) + "/" + xmlkey
        
        return self.__xmlKey

    # NOTE: there could be a concurency error when one threads adds the extend while other thread closes all the connections
    # so there would be extent request in the thread but all threads were close. So then waitTillUploadComplete hang
    def close(self):
        """Closes the channel, sending all upload threads signal to end their operation"""
        logging.debug("Closing the upload threads, End signal message was sent")
        for thread in self.__workThreads:
            self.__uploadQueue.put( None )
   

        