import os 
import sys


import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

import threading
import Queue
import DataExtent
import time
import tempfile
import warnings
import logging

import sys
import os
import win32api
import MigrateExceptions
import subprocess
import re

import logging


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
       
       linktimeexp_seconds = 1361188806
       urldelete = self.__S3.generate_url( linktimeexp_seconds, method='DELETE', bucket=self.__bucket,  key=self.__xmlKey, force_http=False)
       selfdestruct = '\n\t\t </importer> \n\t\t  <self-destruct-url>'+urldelete.replace('&' ,'&amp;')+'</self-destruct-url> '
       self.__file.write(selfdestruct)

       importvol = '\n\t\t <import> \n\t\t\t <size>'+ str(bytesToUpload) + '</size> \n\t\t\t <volume-size>' + str(resultingSizeGb) + '</volume-size>' + '\n\t\t\t <parts count="'+str(fragmentCount)+'">'
       self.__file.write(importvol)
         
       return

    def addUploadedPart(self , index , rangeStart , rangeEnd , keyName):
        
        linktimeexp_seconds = 1361188806

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

class S3UploadThread(threading.Thread):
    """thread making all uploading works"""
    def __init__(self , queue , threadId):
        self.__uploadQueue = queue
        #thread id , for troubleshooting purposes
        self.__threadId = threadId
        return super(S3UploadThread,self).__init__()

    def run(self):
        while 1:

            (bucket , keyname, size, data_getter) = self.__uploadQueue.get()

            #TODO: catch all the exceptions here, notify on them, retry.
            #TODO: we should handle the reuploading

            try:
            # s3 key means file in the bucket (directory)
                s3key = Key(bucket , keyname)
            # we upload the contents

                s3key.set_contents_from_string(str(data_getter)[0:size], replace=True, policy=None, md5=None, reduced_redundancy=False, encrypt_key=False)
                s3key.close()
            except Exception as e:
                #reput the task into the queue
                self.__uploadQueue.put( (bucket , keyname, size, data_getter) )
                logging.warning("Failed to upload data: %s/%s ", str(bucket), keyname )
            self.__uploadQueue.task_done()
        

#TODO: inherit from kinda base one
#TODO: descibe the stack backup source->transfer target->media->channel
class S3UploadChannel(object):
    """channel for s3 uploading"""


    def __init__(self, bucket, awskey, awssercret , resultDiskSizeBytes , location = '' , keynameBase = None, diskType = 'VHD' , uploadThreads=4 , queueSize=16):
        self.__uploadQueue = Queue.Queue(queueSize)
        
        boto.set_file_logger("boto", "..\\..\\logs\\boto.log", level=logging.DEBUG)

        awsregion = location
        if location == 'us-east-1':
           awsregion = ''  

        #TODO: catch kinda exception here if not connected. shouldn't last too long
        hostname = 's3.amazonaws.com'
        if awsregion:
            hostname = 's3-'+awsregion+'.amazonaws.com'
        self.__S3 = S3Connection(awskey, awssercret, is_secure=True, host=hostname)
        
        #TODO: check it is available, check the zone also
        self.__bucketName = bucket
        try:
            self.__bucket = self.__S3.get_bucket(self.__bucketName)
        except Exception as ex:
            logging.warning("Cannot find the bucket. Creating a new one");
            try:
                self.__bucket = self.__S3.create_bucket(self.__bucketName , location=awsregion)
            except:
                logging.error("Couldn't both find and create the bucket " + self.__bucketName + " in a region " + location + "." + "It's possible the bucket with the same name exists but in another region. Try to specify different bucekt name for the upload")
                #TODO: make kinda better exception
                raise BaseException
    

        self.__region = awsregion

        self.__diskType = diskType

        self.__uploadedSize = 0
        self.__xmlKey = None
       
        gigabyte = 1024*1024*1024
        self.__volumeToAllocateGb = int((resultDiskSizeBytes+gigabyte-1)/gigabyte)
        
        now = time.localtime()

        if keynameBase:
            self.__keyBase = keynameBase
        else:
            # migrate + number of seconds since 1980
            self.__keyBase = "Migrate" +str(long(time.mktime(now)))

        #dictionary by the start of the block
        self.__fragmentDictionary = dict()
        #initializing a number of threads
        self.__workThreads = list()
        i = 0
        while i < uploadThreads:
            thread = S3UploadThread(self.__uploadQueue , i)
            thread.start()
            self.__workThreads.append(thread )
            i = i + 1
        return

        logging.info("Succesfully created an upload channel to S3 bucket " + self.__bucketName  + " at " +  location)

    # this one is async
    def uploadData(self, extent):       
       #TODO: monitor the queue sizes
       start = extent.getStart()
       size = extent.getSize()
       keyname =  self.__keyBase+"/temppart"+str(start)

       self.__uploadQueue.put( (self.__bucket , keyname, size, extent.getData() ) )
       # todo: log
       #TODO: make this tuple more flexible
       self.__fragmentDictionary[start] = (keyname , size)
       #TODO: add whenever it was really uploaded! Not justa dded to the queue
       self.__uploadedSize = self.__uploadedSize + size

       return 

    def getDataTransferRate():
        #TODO: add transfer rate
        return 

    #gets the overall size of data uploaded
    def getOverallDataTransfered():
        return self.__uploadedSize 

    # wait uploaded all needed
    def waitTillUploadComplete(self):
        self.__uploadQueue.join()
        return

    # confirm good upload. uploads resulting xml then, returns the id of the upload done
    def confirm(self):
        # generate the XML file then:

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

        keyprefix = "result"
        xmlkey = self.__keyBase+"/"+keyprefix+"manifest.xml"
        

        manifest = S3ManfiestBuilder( xmltempfile , xmlkey , self.__bucketName, self.__S3)

        manifest.buildHeader(self.__uploadedSize , self.__volumeToAllocateGb , fragment_count)
        

        #TODO: faster the solution by re-sorting the dictionary or have an another container
        for start in starts:
            (keyname , size) = self.__fragmentDictionary[start]
            
            # here we rename the parts to reflect their sequence numbers
            newkeyname = self.__keyBase+"/"+keyprefix+".part"+str(fragment_index);
            self.__bucket.copy_key(newkeyname, self.__bucketName, keyname)
            self.__bucket.delete_key(keyname)

            manifest.addUploadedPart(fragment_index, start , start+size , newkeyname)
            fragment_index = fragment_index + 1
        
        manifest.finalize()

        #TODO: try again with close
        # now we ignore the xml so ec2import will create it itself but will get the our data
        
       # s3key = Key(self.__bucket , xmlkey)
       # s3key.set_contents_from_filename(xmltempfile);
       # s3key.close()
        
        self.__xmlKey = 'https://s3.amazonaws.com/' + str(self.__bucketName) + "/" + xmlkey
        
        return self.__xmlKey

   

        