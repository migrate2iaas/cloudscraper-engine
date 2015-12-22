"""
EC2MinipadInstanceGenerator
~~~~~~~~~~~~~~~~~

This module provides EC2MinipadInstanceGenerator class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('.\..')

import logging
import traceback
import urllib2

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
        volume = self.__ec2Connnection.create_volume(self.__diskSize , self.__zone, volume_type = self.__volumeType)
        time.sleep(self.__operationsDelay) #todo: wait till volume is ready
        return volume.id

    def attachDiskToMinipad(self, diskid):
        """we just wait here to ensure vm is ready"""
        
        if self.__minipadVM:
            self.__ec2Connnection.attach_volume(diskid , self.__minipadVM.id , '/dev/xvdf')
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
            try:
                self.__ec2Connnection.authorize_security_group(group_id = self.__securityGroup , ip_protocol='tcp', from_port = 80, to_port = 80, cidr_ip = '0.0.0.0/0')
            except Exception as e:
                logging.warn("!Cannot add security rules to the group " + str(self.__securityGroup) + " Reason: " + str(e))
        
        reservation = self.__ec2Connnection.run_instances(self.__ami, placement = self.__zone,\
            instance_type=self.__instanceType , security_group_ids = [self.__securityGroup] , subnet_id = self.__subnet) 
        self.__minipadVM = reservation.instances[0]

        timeout = 180
        sleeptime = 30*1 # check every 30 sec
        while timeout > 0:
            time.sleep(sleeptime)
            timeout = timeout - sleeptime

        self.__minipadVM.update()
        return self.__minipadVM.ip_address

    def destroyMinipadServer(self):
        """ to implement in the inherited class """
        # TODO: add it to kinda destructor
        return

    def createVM(self ,disk ,name):
        """ to implement in the inherited class 
        
        return server ip
        """
        self.__ec2Connnection.create_tags( [self.__minipadVM.id], {'Name':name} )
        self.__minipadVM.stop()

        #TODO: wait here till stopped
        time.sleep(180)
        #detach all volumes from instance
        volumes = [v for v in self.__ec2Connnection.get_all_volumes() if v.attach_data.instance_id == self.__minipadVM.id]
        for vol in volumes:
            logging.debug("Detaching volume " + vol.id)
            self.__ec2Connnection.detach_volume(vol.id)

        #TODO: wait here till detached
        time.sleep(300)
        self.__ec2Connnection.attach_volume(disk , self.__minipadVM.id , "/dev/sda1")

        #TODO: change disks here
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

    def waitTillVMBuilt(self , vmid, timeout=3*60):
        sleeptime = 60*1 # check every minute
        logging.info(">>> Waiting till Ec2 Cloudscraper VM is ready")
        while timeout > 0:
            timeout = timeout - sleeptime
            self.__minipadVM.update()
            logging.info("VM state is " + self.__minipadVM.state)
            if self.__minipadVM.state == "running":
                break
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
        self.__builtTimeOutSec = 480
        self.__serviceStartTimeout = 330
        self.__operationsDelay = 60

        super(EC2MinipadInstanceGenerator, self).__init__()
   
        
    def startConversion(self,image , ip , import_type = 'ImportInstance' , server_port = 80):
        """override proxy. it waits till server is built and only then starts the conversion"""
        self.waitTillVMBuilt(self.__minipadVM.id, timeout = self.__builtTimeOutSec ) #legacy stuff, should remove
                
        logging.info("Waiting till service is on")
        #extra wait for service availability
        time.sleep(self.__serviceStartTimeout)

        return super(EC2MinipadInstanceGenerator, self).startConversion(image, ip , import_type , server_port)


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
            logging.warning("!Cannot get XML manifest file from intermediate storage. Possibly the storage is inaccessible. " + str(e))


    def makeInstanceFromImage(self , imageid, initialconfig, instancename, s3owner = "", s3key = "", temp_local_image_path = "" , image_file_size = 0, volume_size_bytes = 0 , imagetype='RAW'):
        """makes instance based on image id - link to public image"""
        self.getDiskSize(imageid)
        if s3owner:
            self.__user = s3owner
        if s3key:
            self.__password = s3key
            self.__ec2Connnection = boto.ec2.connect_to_region(self.__region,aws_access_key_id=self.__user,aws_secret_access_key=self.__password)
        #TODO: delete instance on exception
        return super(EC2MinipadInstanceGenerator, self).makeInstanceFromImage(imageid, initialconfig, instancename)

    def makeVolumeFromImage(self , imageid, initialconfig, instancename):
        """makes volume based on image id - link to public image"""
        return None
        #TODO: implement
