# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from boto.ec2.instance import *

import logging
import traceback

import socket

#TODO: make base class for instances
#it's about to extend
class EC2Instance(object):
    """Class representing EC2 virutal server (instance)"""

    def __init__(self , instance_id , user , password , region):
        #TODO: make ec2 connection here
        self.__user = user
        self.__password = password
        self.__instanceId = instance_id
        self.__ec2Connnection = boto.ec2.connect_to_region(region,aws_access_key_id=user,aws_secret_access_key=password)
        
        return 

    def run(self):
        self.__ec2Connnection.start_instances([self.__instanceId])
        return

    def stop(self):
        self.__ec2Connnection.stop_instances([self.__instanceId] , force=True)
        return

    def __str__(self):
        return "EC2 instance ID="+str(self.__instanceId)

    def checkAlive(self, timeout = 180):
        """
        Performs RDP check for an instance
        Args:
            timeout: int - is timeout to wait in seconds
        """
        instance = boto.ec2.instance.Instance(self.__ec2Connnection)
        instance.id = self.__instanceId
        instance.update(True)
        ip = instance.ip_address
        port = 3389

        try:
            sock = socket.create_connection((ip,port) , timeout)
            sock.close()
            return True
        except Exception as e:
            logging.error("!!!ERROR: Failed to probe the remote server for RDP connection!")
            logging.error("!!!ERROR:" + str(e))
            logging.error(traceback.format_exc())
            return False

        return True

    def attachDataVolume(self):
        return