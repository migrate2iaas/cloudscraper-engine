
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

import EC2Instance
import EC2InstanceGenerator
import logging

class ConfigTest(object):
    def __init__(self , arch, zone , file):
        self.__machineArch = arch
        self.__ec2Zone = zone
        self.__tmpLocalFile = file

    def getArch(self):
        return  self.__machineArch

    def getZone(self):
        return self.__ec2Zone

    def getLocalDiskFile(self):
        return self.__tmpLocalFile

class EC2Instance_test(unittest.TestCase):
    
    def setUp(self):
        #TODO: make test account!
        self.__key = 'AKIAIY2X62QVIHOPEFEQ'
        self.__secret = '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'
        self.__channel = None

        logging.basicConfig(format='%(asctime)s %(message)s' , filename='ec2instance.log',level=logging.DEBUG)
        return
    
    
    def test_fullvhd(self):
        
        filename = 'E:\\vms\\2003r2\\win2003r2_32.vhd'
        size = 20*1024*1024*1024
        bucket = 'feoffuseastconversiontest'
        self.__channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size)
        
        channel = self.__channel

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
        image_id = channel.confirm()
 
        generator = EC2InstanceGenerator.EC2InstanceGenerator("us-east-1")
        instance = generator.makeInstanceFromImage(image_id, ConfigTest("i386", "us-east-1a" , filename) , self.__key , self.__secret , filename)
        self.assertIsNotNone(instance)

    def test_fullvhdeuro(self):
         
        
        filename = 'E:\\vms\\2003r2\\win2003r2_32.vhd'
        size = 20*1024*1024*1024

        # TDOO: should be 10 mb-aligned

        bucket = 'feoffeuwestconversiontest-new1'
        channel = S3UploadChannel.S3UploadChannel(bucket , self.__key , self.__secret ,  size , "eu-west-1")
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
        image_id = channel.confirm()
        
        generator = EC2InstanceGenerator.EC2InstanceGenerator("eu-west-1")
        instance = generator.makeInstanceFromImage(image_id, ConfigTest("i386", "eu-west-1a" , filename) , self.__key , self.__secret , filename)
        self.assertIsNotNone(instance)

        return

    def tearDown(self):
        self.__channel.close()

if __name__ == '__main__':
    unittest.main()




