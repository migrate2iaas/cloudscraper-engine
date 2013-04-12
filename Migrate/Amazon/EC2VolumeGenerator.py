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
import EC2Instance
import time

class EC2VolumeGenerator(object):
    """generator class for ec2 volumes"""

    def __init__(self , region, retries=5):

        self.__region = region
        self.__retryCount = retries

    # makes volume from upload id (xml)
    #def makeVolumeFromImage(self, uploadid):

