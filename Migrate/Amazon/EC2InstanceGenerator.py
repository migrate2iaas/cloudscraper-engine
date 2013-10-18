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

import logging
import EC2Instance
import time
import traceback
import datetime

import EC2ImportConnection
from EC2VolumeGenerator import getImageDataFromXml

class EC2InstanceGenerator(object):
    """generator class for ec2 instances"""

    def __init__(self , region, retries=1):

        self.__region = region
        self.__retryCount = retries


     # marks the data uploaded as system disk, should be called(?) after the upload is confirmed
    #TODO: should pass name here
    def makeInstanceFromImage(self , imageid , initialconfig, s3owner, s3key, temp_local_image_path , image_file_size = 0, volume_size_bytes = 0, imagetype='VHD'):

        #TODO: add machine name so it could be added via tags
        #NOTE: should download imageid of no image_file_size or volume_size_bytes specified

        windir = os.environ['windir']

        xml = imageid
        linktimeexp_seconds = 60*60*24*15 # 15 days

        S3 = S3Connection(s3owner, s3key, is_secure=True)
        parsedurl = xml[xml.find('.com'):].split('/' , 2)
        bucketname = parsedurl[1]
        keyname = parsedurl[2]

        logging.debug("Manifest xml is in bucket " + bucketname + " , key " + keyname) 

        xmlurl = S3.generate_url( linktimeexp_seconds, method='GET', bucket=bucketname, key=keyname, force_http=False)

        ec2region = self.__region
        machine_arch = initialconfig.getArch()
        ec2zone = initialconfig.getZone()

        securitygroup = initialconfig.getSecurity()
        instancetype = initialconfig.getInstanceType()

        tmp_vmdk_file = temp_local_image_path
      
        if image_file_size == 0 and temp_local_image_path:
            if os.path.exists(temp_local_image_path):
                image_file_size = os.stat(temp_local_image_path).st_size

        if volume_size_bytes == 0 or image_file_size == 0:
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
     

        connection = EC2ImportConnection.EC2ImportConnection(s3owner, s3key, ec2region)

        retry = 0
        # trying to get the import working for the several times
        while retry < self.__retryCount:
            retry = retry + 1

            import_task_id = "" 
            # waiting till the process completes        
            import_task = None
            try:
                import_task = connection.import_instance(xmlurl, image_file_size , imagetype , ec2zone ,\
                                                         newvolsize , securitygroup , instancetype , machine_arch , "cloudscraper-"+str(datetime.date.today()) )
                
            except Exception as e:
                logging.error("!!!ERROR: Couldn't start volume conversion!")
                logging.error("!!!ERROR:" + str(e))
                logging.error(traceback.format_exc())
                return None

            import_task_id = import_task.conversion_task_id
            if import_task_id:
                logging.info ("Conversion task "+ str(import_task_id) + " created")
                logging.info (">>>>>>>>>>>>>>> System volume has been uploaded, now it's converted by the Amazon EC2 to run as EC2 instance (it may take up to hour, be patient).")
                logging.info ("Waiting for system volume conversion to complete")
                #
                while 1:
                    
                    import_task.update()
                    importstatus = import_task.get_status()
                    logging.debug ("Current state is " + importstatus) 
                    logging.debug ("Current import task is " + repr(import_task)) 

                    if importstatus == "active" or importstatus == "pending":
                       # logging.info("% Conversion Progress: " + match.group(1) + "%");
                        time.sleep(30) #30 sec
                        continue
                    if importstatus == "completed":
                        resultingtask = connection.get_import_tasks([import_task_id])[0]
                        instanceid = resultingtask.get_resulting_id()
                        logging.info("==========================================================================") 
                        logging.info(">>> The system instance " + instanceid + " has been successfully imported") 
                        logging.info(">>> It could be configured and started via AWS EC2 management console") 
                        logging.info("==========================================================================") 

                        connection.create_tags([instanceid] , {"Name":"cloudscraper-"+str(datetime.date.today()) , "MigrationDate":str(datetime.date.today())} )
                        return EC2Instance.EC2Instance(instanceid , s3owner , s3key , self.__region)
                    if importstatus == "cancelled":
                        logging.error("!!!ERROR: The import task was cancelled by AWS. Reason: ") 
                        logging.error("!!! " + import_task.get_message()) 
                        return None

        return None