
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
    def test_a(self):
        """test1 desctiption"""
        size = long(1024*1024)*1024*10

        try:
            channel = GlanceUploadChannel.GlanceUploadChannel(size , disk_format="raw", server_url="http://87.237.203.66:5000/v2.0" , username="migrate2iaas" , tennant_name="migrate2iaas" , password = "migrate2iaas" )
            # https://eu01.webzillafiles.com:8080/v1/WEBZILLA_e99d95f8cde748b7b7bd86b3e9ba8ab4/testcontainer1/testdisk1_3gb")
            #"swift://2344:3186:icafLFsmAOswwISn@auth.nl01.cloud.webzilla.com:5000/v2.0/cloudscraper-test12/12312RCF2_data_2015-08-25"
            channel.waitTillUploadComplete()
            image_id = channel.confirm()
        except Exception as e:
            logging.error(e)
            raise

        return

    def test_copy_from(self):
        """test1 desctiption"""
        filename = 'E:\\win2003.qcow2'
        size = os.stat(filename).st_size
        size = long(1024*1024)*1024*10

        channel = GlanceUploadChannel.GlanceUploadChannel(size , disk_format="raw", server_url="https://auth.nl01.cloud.webzilla.com:5000/v2.0" , username="3186" , tennant_name="2344" , password = "QpLQCTrJjeoWNJaf")
        channel.initStorage("https://eu01.webzillafiles.com:8080/v1/WEBZILLA_e99d95f8cde748b7b7bd86b3e9ba8ab4/testcontainer1/testdisk1")
        # https://eu01.webzillafiles.com:8080/v1/WEBZILLA_e99d95f8cde748b7b7bd86b3e9ba8ab4/testcontainer1/testdisk1_3gb")
        #"swift://2344:3186:icafLFsmAOswwISn@auth.nl01.cloud.webzilla.com:5000/v2.0/cloudscraper-test12/12312RCF2_data_2015-08-25"
        channel.waitTillUploadComplete()
        image_id = channel.confirm()

        return

    def test_direct_large(self):
        """test1 desctiption"""
        
        filename = 'E:\\vms\\vbox2008r2\\win2008r2.vhd'
        size = os.stat(filename).st_size
        #size = long(1024*1024)*412*1024

        channel = GlanceUploadChannel.GlanceUploadChannel(size , disk_format="raw", server_url="https://auth.nl01.cloud.webzilla.com:5000/v2.0" , username="3186" , tennant_name="2344" , password = "QpLQCTrJjeoWNJaf" , version="2")
        channel.initStorage()

        file = open(filename , "rb")
        datasize = channel.getTransferChunkSize()
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