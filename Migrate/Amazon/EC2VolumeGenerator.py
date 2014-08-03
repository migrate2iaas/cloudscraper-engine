# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import os
import sys
import shutil
import re
import subprocess

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

#to ec2 volume. in order to attach
import boto.ec2.volume
import boto.connection

# use AWSQueryConnection to be able to directly call the EC2 interfaces

import logging
import EC2Volume
import time
import EC2ImportConnection
import traceback
import datetime


def getImageDataFromXml(bucket, keyname, xml):
    """returns tuple (volume-size-bytes,image-size-bytes,image-file-type)"""
    gb = 1024*1024*1024
    volume_size_bytes = 0
    image_file_size = 0
    imagetype = ''
    
    if bucket:
        key = bucket.get_key(keyname)
        if key:
            xmlheader = key.read(4096)
            (head, sep ,tail) = xmlheader.partition("<file-format>")
            if tail:
                (head, sep ,tail) = tail.partition("</file-format>")
                imagetype = head

            (head, sep ,tail) = xmlheader.partition("<size>")
            if tail:
                (head, sep ,tail) = tail.partition("</size>")
                image_file_size = int(head , base = 10)
                logging.debug("The image of size " + str(image_file_size))
            else:
                logging.warning("!Couldn't parse the xml describing the import done")
            (head, sep ,tail) = xmlheader.partition("<volume-size>")
            if tail:
                (head, sep ,tail) = tail.partition("</volume-size>")
                volume_size_bytes = int(head , base = 10) * gb
                logging.debug("The volume would be of size " + str(volume_size_bytes))
            else:
                logging.warning("!Couldn't parse the xml describing the import done")
        else:
            logging.error("!!!ERROR: Cannot find " + xml + " describing the image uploaded") 
    else:
        logging.error("!!!ERROR: Couldn't access bucket " + str(bucket) + " to find " + xml + " describing the image uploaded")

    return (volume_size_bytes, image_file_size , imagetype)



class EC2VolumeGenerator(object):
    """generator class for ec2 volumes"""

    def __init__(self , region, retries=1):

        self.__region = region
        self.__retryCount = retries

    # makes volume from upload id (xml)
    def makeVolumeFromImage(self, imageid, initialconfig, s3owner, s3key, temp_local_image_path , image_file_size = 0 , volume_size_bytes = 0 , imagetype = 'VHD', walrus = False , walrus_path = "/services/WalrusBackend" , eucalypus_host="" , eucalyptus_port = 8773 , eucalyptus_path = "/services/Imaging"):

        windir = os.environ['windir']

        xml = imageid
        linktimeexp_seconds = 60*60*24*15 # 15 days

        S3 = None
        if walrus:
            S3 = boto.connect_s3(aws_access_key_id=s3owner,
            aws_secret_access_key=s3key,
            is_secure=False,
            host=location,
            port=8773,
            path=walrus_path,
            calling_format=OrdinaryCallingFormat())
        else:
            S3 = S3Connection(s3owner, s3key, is_secure=True)

        parsedurl = xml[xml.find('.com'):].split('/' , 2)
        bucketname = parsedurl[1]
        keyname = parsedurl[2]

        gb = 1024*1024*1024

        logging.debug("Manifest xml is in bucket " + bucketname + " , key " + keyname) 

        xmlurl = S3.generate_url( linktimeexp_seconds, method='GET', bucket=bucketname, key=keyname, force_http=False)
        #TODO: download the image-id xml if no volume_size and image_file_size given
        if image_file_size == 0 and temp_local_image_path:
             if os.path.exists(temp_local_image_path):
                image_file_size = os.stat(temp_local_image_path).st_size

        if volume_size_bytes == 0 or image_file_size == 0:
            bucket = S3.get_bucket(bucketname)
            (volume_size_bytes , image_file_size , imagetype) = getImageDataFromXml(bucket, keyname, xml)

        scripts_dir = ".\\Amazon"

        ec2region = self.__region
        machine_arch = initialconfig.getArch()
        ec2zone = initialconfig.getZone()

        gb = 1024*1024*1024
        newvolsize = (volume_size_bytes + gb - 1) / gb

        tmp_vmdk_file = temp_local_image_path
     
        if walrus:
            connection = EC2ImportConnection.EC2ImportConnection(s3owner, s3key, ec2region , host = eucalyptus_host , port = eucalyptus_port , path = eucalyptus_path , eucalyptus = walrus , is_secure=False)
        else:
            connection = EC2ImportConnection.EC2ImportConnection(s3owner, s3key, ec2region)

        retry = 0
        # trying to get the import working for the several times
        while retry < self.__retryCount:
            retry = retry + 1

            import_task = None
            try:
                import_task = connection.import_volume(xmlurl, image_file_size , imagetype.upper(), ec2zone , newvolsize , "cloudscraper"+str(datetime.date.today()) )
            except Exception as e:
                logging.error("!!!ERROR: Couldn't start volume conversion!")
                logging.error("!!!ERROR:" + str(e))
                logging.error(traceback.format_exc())
                return None

            import_task_id = import_task.conversion_task_id
            if import_task_id:
                logging.info ("Conversion task "+ str(import_task_id) + " created")
                logging.info (">>>>>>>>>>>>>>> Data volume has been uploaded, now it's converted by the Amazon EC2 to EBS volume (it may take up to hour, be patient).")
                logging.info ("Waiting for system volume conversion to complete")
                #
            while 1:
                import_task.update()
                importstatus = import_task.get_status()
                logging.debug ("Current state is " + importstatus) 
                if importstatus == "active" or importstatus == "pending":
                    #logging.info("% Conversion Progress: " + match.group(1) + "%") 
                    time.sleep(30) #30 sec
                    continue
                if importstatus == "completed":
                    logging.info("Conversion done")
                    
                    resultingtask = connection.get_import_tasks([import_task_id])[0]
                    volumeid = resultingtask.get_resulting_id()

                    logging.info("==========================================================================") 
                    logging.info(">>> The data volume " + volumeid + " has been successfully imported") 
                    logging.info(">>> It could be attached to your instance via AWS EC2 management console") 
                    logging.info("==========================================================================") 

                    return EC2Volume.EC2Volume(volumeid)
                if importstatus == "cancelled":
                    logging.error("!!!ERROR: The import task was cancelled by AWS, the reason is: " + import_task.get_message()) 
                    return None

        return None

