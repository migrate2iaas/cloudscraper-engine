
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

#TODO: make base class for instances
class EC2Instance(object):
    """description of class"""

    def __init__(self , instanceId):

        #TODO: make ec2 connection here

        self.__instanceId = instanceId

        return 
