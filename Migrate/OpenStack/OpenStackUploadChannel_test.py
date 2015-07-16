

import sys

sys.path.append(sys.path[0]+'\.\..')
sys.path.append('.\..\OpenStack')
sys.path.append('.\OpenStack')



import unittest
import logging
import traceback
import sys

import os
import DataExtent
import time
#import OpenStackUploadChannel
from OpenStack import OpenStackUploadChannel


class OpenStackUploadChannel_test(unittest.TestCase):
    """FtpUploadChannel class unittest"""

    #--------------------- Tests:
    
    def test_medium_vhd(self):
        """test1 desctiption"""
        return
        filename = 'E:\\vhdtest1.vhd'
        size = os.stat(filename).st_size

        channel = OpenStackUploadChannel.OpenStackUploadChannel(size , server_url="https://auth.nl01.cloud.webzilla.com:5000/v2.0" , username="3186" , tennant_name="2344" , password = "QpLQCTrJjeoWNJaf")
        channel.initStorage()

        file = open(filename , "rb")
        datasize = 64*1024 
        dataplace = 0
        while 1:
            try:
                data = file.read(datasize)
            except EOFError:
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataplace = dataplace + len(data)
            dataext.setData(data)
            channel.uploadData(dataext)

        channel.waitTillUploadComplete()
        channel.confirm()

        return

    def test_compressed_vhd(self):
        """test1 desctiption"""

        filename = 'E:\\openstack-ubuntu.tar.gz'
        size = os.stat(filename).st_size

        channel = OpenStackUploadChannel.OpenStackUploadChannel(size , server_url="https://auth.nl01.cloud.webzilla.com:5000/v2.0" , username="3186" , tennant_name="2344" , password = "QpLQCTrJjeoWNJaf",\
            disk_format="qcow2", container_format="ovf" , image_name="test-compressed-native-ovf1")
        channel.initStorage()

        file = open(filename , "rb")
        datasize = 64*1024 
        dataplace = 0
        while 1:
            try:
                data = file.read(datasize)
            except EOFError:
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataplace = dataplace + len(data)
            dataext.setData(data)
            channel.uploadData(dataext)

        channel.waitTillUploadComplete()
        channel.confirm()

        return

    #---------------------- Init and Deinit:
    def setUp(self):
        """sets up the object before any single test"""
        
        
        return

    def tearDown(self):
        """frees the resources after any single test"""
        
        return 

    @classmethod
    def setUpClass(cls):
        """sets up the environment before its testing begun"""
        # use cls.__something if you wish to add some static objects into it
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.DEBUG)

    @classmethod
    def tearDownClass(cls):
        """frees any global resources allocated for the test """
        return

       
if __name__ == '__main__':
    unittest.main()