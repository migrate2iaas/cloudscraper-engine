"""
CloudSigmaUploadChannel_test
~~~~~~~~~~~~~~~~~

Unittest for CloudSigmaUploadChannel class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('.\..')
sys.path.append('.\..\CloudSigma')
sys.path.append('.\CloudSigma')

import unittest
import logging
import traceback
import stat
import os
import DataExtent

import CloudSigmaUploadChannel



class CloudSigmaUploadChannel_test(unittest.TestCase):
    """CloudSigmaUploadChannel class unittest"""

    #--------------------- Tests:
    
    def test_upload(self):
        """upload """
        filename = "F:\\drive3.raw"
        size = os.stat(filename).st_size
        file = open(filename, "rb")
        self.createChannel(size, False)
        
        dataplace = 0
        while 1:
            try:
                data = file.read(self.__channel.getTransferChunkSize())
            except: 
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataext.setData(data)
            dataplace = dataplace + len(data)
            self.__channel.uploadData(dataext)
        file.close()

        self.__channel.waitTillUploadComplete()    
        diskid = self.__channel.confirm()
        logging.info("Disk "+ diskid+ " was uploaded!") 

        return

    def test_reupload(self):
        """reupload """
        filename = "F:\\drive3.raw"
        size = os.stat(filename).st_size
        file = open(filename, "rb")
        self.createChannel(size, False)

        dataplace = 0
        while 1:
            try:
                data = file.read(self.__channel.getTransferChunkSize())
            except:
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataext.setData(data)
            self.__channel.uploadData(dataext)
            if dataplace * 2 > size:
                break
            dataplace = dataplace + len(data)
        file.close()

        #self.__channel.waitTillUploadComplete()    
        #diskid = self.__channel.confirm()
        logging.info("Disk "+ diskid+ " upload was interrupted!") 

        self.assertGreater(self.__channel.getOverallDataTransfered() , size/2)

        self.__channel.close()
        self.createChannel(size, True , diskid)

        dataplace = 0
        while 1:
            try:
                data = file.read(self.__channel.getTransferChunkSize())
            except:
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataext.setData(data)
            dataplace = dataplace + len(data)
            self.__channel.uploadData(dataext)
        file.close()

        self.__channel.waitTillUploadComplete()    
        diskid = self.__channel.confirm()

        self.assertLessEqual(self.__channel.getOverallDataTransfered() , size/2)
        logging.info("Disk "+ diskid+ " was reuploaded!") 

        return

    def createChannel(self, size , reupload , diskid = None):
        self.__channel = CloudSigmaUploadChannel.CloudSigmaUploadChannel(size , self.__region , self.__user , self.__secret , resume_upload = reupload , drive_uuid = diskid)
        self.__channel.initStorage()

    #---------------------- Init and Deinit:
    def setUp(self):
        """sets up the object before any single test"""
        self.__user = 'feoff@migrate2iaas.com'
        self.__secret = 'BolshoyAdmin123'
        self.__channel = None
        self.__region = 'zrh'
        return

    def tearDown(self):
        """frees the resources after any single test"""
        if self.__channel:
            self.__channel.close()
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