
import sys


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

class S3UploadChannel_test(unittest.TestCase):
    
    def setUp(self):
        #TODO: make test account!
        self.__key = 'AKIAIY2X62QVIHOPEFEQ'
        self.__secret = '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'
        return
        

    def test_file_useast(self):
        size = 1024*1024*1024
        bucket = 'feoffuseastfiletest'
        channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size)
        
        file = open('D:\\log.txt' , "rb")
        data = file.read()
        dataext = DataExtent.DataExtent(0 , len(data))
        dataext.setData(data)

        channel.uploadData(dataext)
        channel.waitTillUploadComplete()
        channel.confirm()
        

    def test_file_euro(self):
        size = 1024*1024*1024
        bucket = 'feoffuseastfiletest.s3-eu-west-1a'
        channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size , 'eu-west-1')
        
        file = open('D:\\log.txt' , "rb")
        data = file.read()
        dataext = DataExtent.DataExtent(0 , len(data))
        dataext.setData(data)
        file.close()
        channel.uploadData(dataext)
        channel.waitTillUploadComplete()
        channel.confirm()

    def test_fullvhd(self):
        
        filename = 'E:\\vms\\2003r2\\win2003r2_32.vhd'
        size = 20*1024*1024*1024
        bucket = 'feoffuseastfiletestvhd'
        channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size)
        
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

if __name__ == '__main__':
    unittest.main()




