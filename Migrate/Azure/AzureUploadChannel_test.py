# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('.\..')
sys.path.append('.\..\Amazon')
sys.path.append('.\..\Windows')
sys.path.append('.\..\ElasticHosts')

sys.path.append('.\Windows')
sys.path.append('.\Amazon')
sys.path.append('.\ElasticHosts')


import AzureUploadChannel
import unittest

class AzureUploadChannel_test(unittest.TestCase):

    def setUp(self):
        self.__key = ''
        self.__secret = ''
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
        self.__channel = AzureUploadChannel.AzureUploadChannel(storage_account , accesskey , sizebytes , container_name , diskname , reupload)
        return

    def test_uploadNulls(self):
        data = bytearray(1024*1024*64)

        self.initChannel("disk" , len(data))

        dataext = DataExtent.DataExtent(dataplace , len(data))
        dataext.setData(data)
        self.__channel.uploadData(dataext)      
        self.__channel.waitTillUploadComplete()    
        diskid = self.__channel.confirm()
        return

    def test_uploadWrong(self):
        return

    def test_uploadEmpty(self):
        return