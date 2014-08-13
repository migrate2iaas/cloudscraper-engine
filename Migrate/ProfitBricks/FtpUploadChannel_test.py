"""
FtpUploadChannel_test
~~~~~~~~~~~~~~~~~

Unittest for CLASS class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys


sys.path.append('.\..')
sys.path.append('.\..\Windows')
sys.path.append('.\..\ProfitBricks')

sys.path.append('.\Windows')
sys.path.append('.\ProfitBricks')

import unittest
import logging
import traceback
import sys
import FtpUploadChannel
import DataExtent
import time

#import CLASS




class FtpUploadChannel_test(unittest.TestCase):
    """FtpUploadChannel class unittest"""

    #--------------------- Tests:
    
    def test_small_vhd(self):
        """test1 desctiption"""

        channel = FtpUploadChannel.FtpUploadChannel("hdd-images/test"+str(time.time())+".vhd", self.__user , self.__password, self.__host)
        
        filename = 'E:\\vhdtest.vhd'
        file = open(filename , "rb")
        datasize = 1024*1024 
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

    def test_tiny_vmdk(self):
        """test2 desctiption"""
        
        channel = FtpUploadChannel.FtpUploadChannel("hdd-images/test"+str(time.time())+".vmdk", self.__user , self.__password, self.__host)
        
        filename = 'E:\\J.vmdk'
        file = open(filename , "rb")
        datasize = 1024*1024 
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
        self.__user = "feoff@migrate2iaas.com"
        self.__password = "BolshoyAdmin123"
        self.__host = "ftp-fkb.profitbricks.com"
        
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