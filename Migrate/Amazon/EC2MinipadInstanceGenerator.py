"""
EC2MinipadInstanceGenerator
~~~~~~~~~~~~~~~~~

This module provides EC2MinipadInstanceGenerator class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback


import os
import sys
import shutil
import re
import subprocess

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from boto.exception import BotoServerError
from boto.s3.connection import OrdinaryCallingFormat

from boto.ec2.instance import *

import logging
import EC2Instance
import time
import traceback
import datetime

import EC2ImportConnection
from EC2VolumeGenerator import getImageDataFromXml

import InstanceGenerator
import MiniPadInstanceGenerator
import EC2Instance

class EC2MinipadInstanceGenerator(MiniPadInstanceGenerator.MiniPadInstanceGenerator):
    """ec2instance generator based on minipad"""

    def createDisk(self , name):
        """adds disk to the vm where image data is stored to"""
        volume = self.__ec2Connnection.create_volume(self.__diskSizeGB , self.__zone, volume_type = self.__volumeType)
        if self.__minipadVM:
            self.__minipadVM.attach_volume(volume.id , '/dev/xvdf')

    def attachDiskToMinipad(self, diskid):
        """we just wait here to ensure disk attached"""
        timeout = 180
        sleeptime = 30*1 # check every 30 sec
        logging.debug("Waiting till disk is attached")
        while timeout > 0:
            time.sleep(sleeptime)
            timeout = timeout - sleeptime
        return

    def initCreate(self , initialconfig):
        """inits the conversion"""
        # add parms like disk sizes here

        # TODO: get from parms
        
        return

    def launchMinipadServer(self):
        """ launch vm """
        if not self.__securityGroup:
            group = self.__ec2Connnection.create_security_group('Cloudscraper-HTTP-'+str(int(time.time())), 'HTTP enabled group')
            group.authorize('tcp', 80, 80, '0.0.0.0/0')
            self.__securityGroup = group.id
        else:
            self.__ec2Connnection.authorize_security_group(group_id = self.__securityGroup , ip_protocol='tcp', from_port = 80, to_port = 80, cidr_ip = '0.0.0.0/0')
        
        reservation = self.__ec2Connnection.run_instances(self.__ami, placement = self.__zone, key_name='Cloudscraper-Minipad-Target',\
            instance_type=self.__instanceType , security_group_ids = [self.__securityGroup] , subnet_id = self.__subnet) 
        self.__minipadVM = reservation.instances()[0]

        return

    def destroyMinipadServer(self):
        """ to implement in the inherited class """
        # TODO: add it to kinda destructor
        return

    def createVM(self ,disk ,name):
        """ to implement in the inherited class 
        
        return server ip
        """
        return EC2Instance.EC2Instance(self.__minipadVM.id, self.__user , self.__password  , self.__region)

    def detachDiskFromMinipad(self , disk):
        """ to implement in the inherited class """
        #Create Disk Backup  https://docs.onapp.com/display/31API/Create+Disk+Backup
        #Convert Backup to Template https://docs.onapp.com/display/31API/Convert+Backup+to+Template
               
        #backups = self.__onapp.backupVM(self.__minipadId)#self.__onapp.backupDisk(disk)
        #if len(backups) == 0:
        #    logging.error("!!!ERROR: disk template creation failed (backup failed)")
        #for backup in backups:
            # assume the last one is ours (it should be one of them all the times, by the way)
        #    bu_id = backup['id']
        #template_id = self.__onapp.createTemplate(self)['id']
        #self.__templateId = template_id
        return

    def waitTillVMBuilt(self , vmid, timeout=30*60):
        sleeptime = 60*1 # check every minute
        logging.info(">>> Waiting till Ec2 Cloudscraper VM is ready")
        while timeout > 0:
            timeout = timeout - sleeptime
            time.sleep(sleeptime)
        return

    def __init__(self, region , ami , key , secret , zone, instance_type, subnet , security_group, volume_type = 'gp2'):
        """
            
        """
        self.__user = key
        self.__password = secret
        self.__ami = ami
        self.__ec2Connnection = boto.ec2.connect_to_region(region,aws_access_key_id=self.__user,aws_secret_access_key=self.__password)
        self.__instance = None
        self.__region = region
        self.__instanceType = instance_type
        self.__subnet = subnet
        self.__securityGroup = security_group
        self.__minipadVM = None
        self.__volumeType = volume_type
        self.__zone = zone

        super(EC2MinipadInstanceGenerator, self).__init__()
   
        
    def startConversion(self,image , ip , import_type = 'ImportInstance' , server_port = 80):
        """override proxy. it waits till server is built and only then starts the conversion"""
        self.waitTillVMBuilt(self.__minipadId, timeout = self.__builtTimeOutSec )
        
        vm = onAppVM(self.__onapp, self.__minipadId)
        logging.debug("Trying to run the VM , in case it's stopped")
        vm.run()
        logging.info("Awaiting till Cloudscraper target VM is alive (echoing Cloudscraper VM RDP port)")
        if vm.checkAlive() == False:
            logging.warn("!Cloudscraper target VM is not repsonding (to RDP port). Misconfiguration is highly possible!")
        
        logging.info("Waiting till service is on")
        #extra wait for service availability
        time.sleep(self.__serviceStartTimeout)

        return super(onAppInstanceGenerator, self).startConversion(image, ip , import_type , server_port)


    def getDiskSize(self, imageid_manifest_link):
        """downloads image link to get the disk size"""
        imageid = imageid_manifest_link
        try:
            response = urllib2.urlopen(imageid)
            xmlheader = response.read()
            (head, sep ,tail) = xmlheader.partition("<volume-size>")
            if tail:
                    (head, sep ,tail) = tail.partition("</volume-size>")
                    self.__diskSize = int(head , base = 10) + 1 # add one so to fit 100%
                    logging.debug("The volume would be of size " + str(self.__diskSize) + " GBs")
            else:
                    logging.warning("!Couldn't parse the xml describing the import done")
        except Exception as e:
            logging.warning("!Cannot get XML manifest file from intermediate storage. Possibly the storage is inaccessible.")


    def makeInstanceFromImage(self , imageid, initialconfig, instancename, s3owner = "", s3key = "", temp_local_image_path = "" , image_file_size = 0, volume_size_bytes = 0):
        """makes instance based on image id - link to public image"""
        self.getDiskSize(imageid)
        gb = 1024*1024*1024
        self.__diskSizeGB = (self.__diskSize + gb - 1) / gb
        return super(EC2MinipadInstanceGenerator, self).makeInstanceFromImage(imageid, initialconfig, instancename)

    def makeVolumeFromImage(self , imageid, initialconfig, instancename):
        """makes volume based on image id - link to public image"""
        return None
        #TODO: implement