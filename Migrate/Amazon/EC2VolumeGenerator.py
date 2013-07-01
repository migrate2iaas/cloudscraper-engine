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

class EC2VolumeGenerator(object):
    """generator class for ec2 volumes"""

    def __init__(self , region, retries=1):

        self.__region = region
        self.__retryCount = retries

    # makes volume from upload id (xml)
    def makeVolumeFromImage(self, imageid, initialconfig, s3owner, s3key, temp_local_image_path , image_file_size = 0 , volume_size_bytes = 0):

        windir = os.environ['windir']

        xml = imageid
        linktimeexp_seconds = 60*60*24*15 # 15 days

        S3 = S3Connection(s3owner, s3key, is_secure=True)
        parsedurl = xml[xml.find('.com'):].split('/' , 3)
        buceketname = parsedurl[1]
        keyname = parsedurl[2]

        logging.debug("Manifest xml is in bucket " + buceketname + " , key " + keyname);

        xmlurl = S3.generate_url( linktimeexp_seconds, method='GET', bucket=buceketname, key=keyname, force_http=False)
        #TODO: download the image-id xml if no volume_size and image_file_size given
        if image_file_size == 0 and temp_local_image_path:
            image_file_size = os.stat(temp_local_image_path).st_size

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

            import_task = None
            try:
                import_task = connection.import_volume(xmlurl, image_file_size , 'VHD' , ec2zone , newvolsize , "cloudscraper"+str(datetime.date.today()) )
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
                logging.debug ("Current state is " + importstatus);
                if importstatus == "active" or importstatus == "pending":
                    #logging.info("% Conversion Progress: " + match.group(1) + "%");
                    time.sleep(30) #30 sec
                    continue
                if importstatus == "completed":
                    logging.info("Conversion done")
                    
                    resultingtask = connection.get_import_tasks([import_task_id])[0]
                    volumeid = resultingtask.get_resulting_id()

                    logging.info("==========================================================================");
                    logging.info(">>> The data volume " + volumeid + " has been successfully imported");
                    logging.info(">>> It could be attached to your instance via AWS EC2 management console");
                    logging.info("==========================================================================");

                    return EC2Volume.EC2Volume(volumeid)
                if importstatus == "cancelled":
                    logging.error("!!!ERROR: The import task was cancelled by AWS, the reason is: " + import_task.get_message());
                    return None

        return None

