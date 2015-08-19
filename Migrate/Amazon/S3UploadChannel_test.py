# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('.\..')
sys.path.append('.\..\Amazon')
sys.path.append('.\..\Windows')

sys.path.append('.\Windows')
sys.path.append('.\Amazon')

import unittest
import WindowsVolume
import WindowsBackupSource
import AdjustedBackupSource
import BackupAdjust
import WindowsVolumeTransferTarget
import WindowsDiskParser
import WindowsVhdMedia
import WindowsDeviceDataTransferProto
import S3UploadChannel
import time
import DataExtent
import logging

class S3UploadChannel_test(unittest.TestCase):
    
    def setUp(self):
        #TODO: make test account!
        self.__key = 'AKIAIY2X62QVIHOPEFEQ'
        self.__secret = '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'
        self.__channel = None
        logging.basicConfig(format='%(asctime)s %(message)s' , filename='s3channel.log',level=logging.DEBUG)
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        return
        

    def test_file_useast(self):
        size = 1024*1024*1024
        bucket = 'feoffuseastfiletest12'
        channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size)
        self.__channel = channel

        file = open('D:\\log.txt' , "rb")
        data = file.read()
        dataext = DataExtent.DataExtent(0 , len(data))
        dataext.setData(data)

        channel.uploadData(dataext)
        channel.waitTillUploadComplete()
        channel.confirm()
       

    #TODO: commit bad these tests
    def notused_test_bad_bucketname2(self):
        size = 1024*1024*1024
        
        bucket = '!~feoffuseastfiletest2.s3-eu-west-1ab'
        channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size , 'eu-west-1')
        self.__channel = channel
        file = open('D:\\log.txt' , "rb")
        data = file.read()
        dataext = DataExtent.DataExtent(0 , len(data))
        dataext.setData(data)
        file.close()
        channel.uploadData(dataext)
        channel.waitTillUploadComplete()
        channel.confirm()

    def notused_test_bad_bucketname1(self):
        size = 1024*1024*1024
        bucket = 'test'
        channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size , 'eu-west-1')
        self.__channel = channel
        file = open('D:\\log.txt' , "rb")
        data = file.read()
        dataext = DataExtent.DataExtent(0 , len(data))
        dataext.setData(data)
        file.close()
        channel.uploadData(dataext)
        channel.waitTillUploadComplete()
        channel.confirm()


    def test_file_euro(self):
        size = 1024*1024*1024
        bucket = 'feoffuseastfiletest2.s3-eu-west-1ab'
        channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size , 'eu-west-1')
        self.__channel = channel
        file = open('D:\\log.txt' , "rb")
        data = file.read()
        dataext = DataExtent.DataExtent(0 , len(data))
        dataext.setData(data)
        file.close()
        channel.uploadData(dataext)
        channel.waitTillUploadComplete()
        channel.confirm()
       

    def test_fullvhd(self):
        
        filename = 'E:\\vhdtest1.vhd'
        size = 20*1024*1024*1024
        bucket = 'feoffuseastfiletestvhd2'
        channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size)
        self.__channel = channel
        #TODO: make more different sizes
        file = open(filename , "rb")
        datasize = 10*1024*1024 #mb
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

    def resumeUpload(self , region , bucket, filename , size):
        channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size, region, None , 'VHD', True)
        self.__channel = channel
        #TODO: make more different sizes
        #TODO: test on file changes 
        #TODO: test on different chunks
        
        datasize = 10*1024*1024 #mb
        sizeiteration = 0
        while 1:
            dataplace = 0
            fileend = False
            sizeiteration = sizeiteration + 1
            datatotransfer = sizeiteration*1024*1024*1024 # gb
            file = open(filename , "rb")
            while 1:
                try:
                    data = file.read(datasize)
                except EOFError:
                    fileend = True
                    break
                if len(data) == 0:
                    fileend = True
                    break
                dataext = DataExtent.DataExtent(dataplace , len(data))
                dataplace = dataplace + len(data)
                dataext.setData(data)
                channel.uploadData(dataext)
                if dataplace > datatotransfer:
                    break

            file.close()
            channel.waitTillUploadComplete()
            if fileend == True:
                break
        
        channel.confirm()
        

    def test_resumeupload_useast(self):
        filename = 'E:\\vhdtest1.vhd'
        size = 20*1024*1024*1024
        bucket = 'feoffuseastfiletestvhd'
        region = ''
        self.resumeUpload(region, bucket , filename , size)
        #NOTE: somehow we should check the performance of this upload is the same as 
    def test_resumeupload_euro(self):
        bucket = 'feoffuseastfiletestvhdeuro'
        region = 'eu-west-1'
        filename = 'E:\\vhdtest.vhd'
        size = 20*1024*1024*1024
        self.resumeUpload(region, bucket , filename , size)

    #TODO: generate new bucket

    def tearDown(self):
        if self.__channel:
            self.__channel.close()


if __name__ == '__main__':
    unittest.main()




