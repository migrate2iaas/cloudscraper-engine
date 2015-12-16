# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from boto.ec2.instance import *

import logging
import traceback
import VmInstance
import socket


class EC2Instance(VmInstance.VmInstance):
    """Class representing EC2 virutal server (instance)"""

    def __init__(self , instance_id , user , password , region):
        """ec2 instance constructor"""
        self.__user = user
        self.__password = password
        self.__instanceId = instance_id
        self.__ec2Connnection = boto.ec2.connect_to_region(region,aws_access_key_id=user,aws_secret_access_key=password)
        
        return 

    def run(self):
        """starts instance"""
        self.__ec2Connnection.start_instances([self.__instanceId])
        return

    def stop(self):
        """stops instance"""
        self.__ec2Connnection.stop_instances([self.__instanceId] , force=True)
        return

    def __str__(self):
        """gets string representation of instance"""
        return "EC2 instance ID="+str(self.__instanceId)

    

    def attachDataVolume(self):
        """attach data volume"""
        #TODO implement
        return


    def getIp(self):
        """gets public ip"""
        instance = boto.ec2.instance.Instance(self.__ec2Connnection)
        instance.id = self.__instanceId
        instance.update(True)
        return instance.ip_address