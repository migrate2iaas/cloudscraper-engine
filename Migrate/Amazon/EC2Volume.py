# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import boto
from boto.ec2.volume import Volume


#TODO: make base class for instances
class EC2Volume(object):
    """represents a volume in AWS"""

    device_letter = 'p'

    def __init__(self , volume_id , key, secret, region):
        self.__volumeId = volume_id
        self.__ec2 = boto.ec2.connect_to_region(region,aws_access_key_id=key,aws_secret_access_key=secret)
        return 

    def getId(self):
        """returns cloud id of the volume"""
        return self.__volumeId

    def attach(self, vm_instance_id):
        """
        Attaches to the machine specified by instance_id
        """
        self.__ec2.attach_volume(self.__volumeId , vm_instance_id, "/dev/xvd"+EC2Volume.device_letter)
        # move from xvdp to xvdf
        EC2Volume.device_letter = chr(ord(EC2Volume.device_letter) - 1)

    def __str__(self):
        return str(self.getId())