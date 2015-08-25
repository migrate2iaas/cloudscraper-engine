
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
from OpenStack import GlanceUploadChannel


class GlanceUploadChannel_test(unittest.TestCase):
    """FtpUploadChannel class unittest"""

    #--------------------- Tests:
    def test_copy_from(self):
        """test1 desctiption"""
        
        filename = 'E:\\win2003.qcow2'
        size = os.stat(filename).st_size

        channel = GlanceUploadChannel.GlanceUploadChannel(size , disk_format="qcow2", server_url="https://auth.nl01.cloud.webzilla.com:5000/v2.0" , username="3186" , tennant_name="2344" , password = "QpLQCTrJjeoWNJaf")
        #channel.initStorage("https://goo.gl/GlI6aX")
        #channel.initStorage("swift://2344:3186:icafLFsmAOswwISn@eu01-auth.webzilla.com:5000/v2.0/cloudscraper-pub/12312RCF2_data_2015-08-25asdasd")
        #"swift://2344:3186:icafLFsmAOswwISn@auth.nl01.cloud.webzilla.com:5000/v2.0/cloudscraper-test12/12312RCF2_data_2015-08-25"
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