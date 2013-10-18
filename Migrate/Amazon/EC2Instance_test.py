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

import EC2Instance
import EC2InstanceGenerator
import logging

import CloudConfig

class ConfigTest(CloudConfig.CloudConfig):
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

    def getNewSystemSize(self):
        return 40

    def getSecurity(self):
        return "default"

    def getInstanceType(self):
        return "m1.medium"

class EC2Instance_test(unittest.TestCase):
    
    def setUp(self):
        #TODO: make test account!
        self.__key = 'AKIAIY2X62QVIHOPEFEQ'
        self.__secret = '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'
        self.__channel = None

        # TODO: make logging to stderr\stdout!

        logging.basicConfig(format='%(asctime)s %(message)s' , filename='ec2instance.log',level=logging.DEBUG)
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        return
    
    
    def test_fullvhd(self):
        
        filename = 'E:\\vms\\2008r2\\win2008r2.vhd'
        size = 136365211648 
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
         
        
        filename = 'E:\\vms\\2008r2\\win2008r2.vhd'
        size = 136365211648 

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

    def test_aregenerate(self):
        image_id = "https://s3.amazonaws.com/feoffuseastconversiontest/Migrate1378927435/imagemanifest.xml"
        generator = EC2InstanceGenerator.EC2InstanceGenerator("us-east-1")
        instance = generator.makeInstanceFromImage(image_id, ConfigTest("i386", "us-east-1a" , "") , self.__key , self.__secret , "")
        self.assertIsNotNone(instance)

        return

    def tearDown(self):
        self.__channel.close()

if __name__ == '__main__':
    unittest.main()




