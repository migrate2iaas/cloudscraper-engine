# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('.\..')
sys.path.append('.\..\Amazon')
sys.path.append('.\..\Windows')
sys.path.append('.\..\Azure')

sys.path.append('.\Windows')
sys.path.append('.\Amazon')
sys.path.append('.\Azure')

import unittest
import logging
import traceback
import stat
import os
import AzureUploadChannel
import unittest
import datetime
import os
import DataExtent
import StringIO

class AzureUploadChannel_test(unittest.TestCase):

    def setUp(self):
        self.__storageAccount = 'migrate2iaastest'
        self.__storageKey = 'U7QrRn7HcTeOKC3CN7OL9cH8sKHvvva8kPP7w4HZqevHPW9yrgO7UgfxmacQzN3y+id9eEj53XB00bpEbRoa4Q=='
        self.__container = 'test'
        self.__channel = None
        self.__region = ''

        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        return

    def tearDown(self):
        if self.__channel:
            self.__channel.close()

    def initChannel(self, diskname, sizebytes, reupload = False):
        self.__channel = AzureUploadChannel.AzureUploadChannel(self.__storageAccount , self.__storageKey  , sizebytes , self.__container , diskname , reupload)
        self.__channel.initStorage()
        return

    def test_uploadNulls(self):
        size = 64*1024*1024
        self.initChannel("testnulls"+str(datetime.datetime.now()) , size, False)
        data = bytearray(size)

        file = StringIO.StringIO(data)

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
        #TODO: check nothing was uploaded
        diskid = self.__channel.confirm()
        self.assertIsNotNone(diskid)
        self.assertEqual(self.__channel.getOverallDataTransfered() , 0)
        return

    def test_uploadWrong(self):
        return

    def test_uploadEmpty(self):
        return
    
    def test_uploadDataVhd(self):
        filename = "F:\\datadisk.vhd"
        size = os.stat(filename).st_size
        file = open(filename, "rb")
        self.initChannel("test"+str(datetime.datetime.now())+".vhd" , size, False)
        
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
        if diskid:
            logging.info("Disk "+ str(diskid) + " was uploaded!") 
        self.assertIsNotNone(diskid)

    def test_uploadSystemVhd(self):
        return
        filename =  'E:\\vms\\2008r2\\win2008r2.vhd'
        size = os.stat(filename).st_size
        file = open(filename, "rb")
        self.initChannel("testsys"+str(datetime.datetime.now())+'.vhd' , size, False)
        
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
        if diskid:
            logging.info("Disk "+ str(diskid) + " was uploaded!") 
        self.assertIsNotNone(diskid)

       
if __name__ == '__main__':
    unittest.main()