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

#TODO: move it away then
import win32file
import win32event
import win32con
import win32security
import win32api
import pywintypes
import ctypes
import winioctlcon
import struct
import datetime
import ntsecuritycon

import EHUploadChannel

class EHUploadChannel_test(unittest.TestCase):
   


    def setUp(self):
        self.__key = '570a0faa-ca17-4689-b06b-3400ce8b5294'
        self.__secret = 'EcbSsaj6YbQnX2qPeYzJBdx4PCtL9zbgk2wEGDcE'
        self.__channel = None
        logging.basicConfig(format='%(asctime)s %(message)s' , filename='elastichosts-channel.log',level=logging.DEBUG)
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        return

    def tearDown(self):
        if self.__channel:
            self.__channel.close()

    # just testing their scripts, too much of hardcode really
    def notusedtest_ShScript(self):
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()

        diskNo = 2
        drivename = "\\\\.\\PhysicalDrive" + str(diskNo)
        hfile = win32file.CreateFile( drivename, win32con.GENERIC_READ| ntsecuritycon.FILE_READ_ATTRIBUTES , win32con. FILE_SHARE_READ, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        rawfile = open("E:\\test.raw" , "wb");
        while 1:
            mb = 1024*1024
            (result , output) = win32file.ReadFile(hfile,mb,None)
            rawfile.write(output)
            if len(output) < mb:
                break;
        rawfile.close()
    
    def test_diskCreate(self):
        self.__channel = EHUploadChannel.EHUploadChannel('' , self.__key, self.__secret, 1024*1024*1024 , 'sat-p' , 'testcreate')
    
    def test_diskUpload(self):
        diskNo = 2
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        drivename = "E:\\diskraw.raw"
        hfile = win32file.CreateFile( drivename, win32con.GENERIC_READ| ntsecuritycon.FILE_READ_ATTRIBUTES , win32con. FILE_SHARE_READ, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        self.__channel = EHUploadChannel.EHUploadChannel('' , self.__key, self.__secret, 256*1024*1024 , 'sat-p' , 'testupload', False)

        dataplace = 0
        while 1:
            mb = 1024*1024
            try:
                (result , data) = win32file.ReadFile(hfile,mb,None)
            except:
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataext.setData(data)
            dataplace = dataplace + len(data)
            self.__channel.uploadData(dataext)
        
        self.__channel.waitTillUploadComplete()    
        diskid = self.__channel.confirm()
        logging.info("Disk "+ diskid+ " was uploaded!");

    def test_diskFullReUpload(self):
        diskNo = 2
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        drivename = "E:\\diskraw.raw"
        hfile = win32file.CreateFile( drivename, win32con.GENERIC_READ| ntsecuritycon.FILE_READ_ATTRIBUTES , win32con. FILE_SHARE_READ, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        diskname = 'testupload'+str(datetime.date.today())
        self.__channel = EHUploadChannel.EHUploadChannel('' , self.__key, self.__secret, 256*1024*1024 , 'sat-p' , diskname , False)

        dataplace = 0
        while 1:
            mb = 4*1024*1024
            try:
                (result , data) = win32file.ReadFile(hfile,mb,None)
            except:
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataext.setData(data)
            dataplace = dataplace + len(data)
            self.__channel.uploadData(dataext)
        

        self.__channel.waitTillUploadComplete()    
        diskid = self.__channel.confirm()
        logging.info("Disk "+ diskid+ " was uploaded!");
        self.__channel.close()
        
        hfile = win32file.CreateFile( drivename, win32con.GENERIC_READ| ntsecuritycon.FILE_READ_ATTRIBUTES , win32con. FILE_SHARE_READ, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        diskname = 'testupload'+str(datetime.date.today())
        self.__channel = EHUploadChannel.EHUploadChannel(diskid , self.__key, self.__secret, 256*1024*1024 , 'sat-p' , diskname , True)
        dataplace = 0
        while 1:
            mb = 4*1024*1024
            try:
                (result , data) = win32file.ReadFile(hfile,mb,None)
            except:
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataext.setData(data)
            dataplace = dataplace + len(data)
            self.__channel.uploadData(dataext)

        self.assertEqual(self.__channel.getOverallDataTransfered() , 0 , "no data should be transfered")


    def test_diskPartialReUpload(self):
        diskNo = 2
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        drivename = "E:\\diskraw.raw"
        hfile = win32file.CreateFile( drivename, win32con.GENERIC_READ| ntsecuritycon.FILE_READ_ATTRIBUTES , win32con. FILE_SHARE_READ, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        diskname = 'testupload'+str(datetime.date.today())
        self.__channel = EHUploadChannel.EHUploadChannel('' , self.__key, self.__secret, 256*1024*1024 , 'sat-p' , diskname , False)

        dataplace = 0
        while 1:
            mb = 4*1024*1024
            try:
                (result , data) = win32file.ReadFile(hfile,mb,None)
            except:
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataext.setData(data)
            dataplace = dataplace + len(data)
            self.__channel.uploadData(dataext)
            if dataplace > 16*mb:
                break
            
        
        self.__channel.waitTillUploadComplete()    
        diskid = self.__channel.confirm()
        logging.info("Disk "+ diskid+ " was uploaded!");
        self.__channel.close()
        
        hfile = win32file.CreateFile( drivename, win32con.GENERIC_READ| ntsecuritycon.FILE_READ_ATTRIBUTES , win32con. FILE_SHARE_READ, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        diskname = 'testupload'+str(datetime.date.today())
        self.__channel = EHUploadChannel.EHUploadChannel(diskid , self.__key, self.__secret, 256*1024*1024 , 'sat-p' , diskname , True)
        dataplace = 0
        while 1:
            mb = 4*1024*1024
            try:
                (result , data) = win32file.ReadFile(hfile,mb,None)
            except:
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataext.setData(data)
            dataplace = dataplace + len(data)
            self.__channel.uploadData(dataext)

        diskid = self.__channel.confirm()
        self.assertLess(self.__channel.getOverallDataTransfered() , 200*1024*1024 , "Less data should be transfered")
        self.assertGreater(self.__channel.getOverallDataTransfered() , 0 , "grater data should be transfered")
   

    def notest_reuploadHome(self):
        #quick test to check if the system is up to work after the ntfs was rebuilt
        # the only thing neede for booting is to fix the ntfs bootsector as it seems to me

        diskNo = 2
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        drivename = "\\\\.\\PhysicalDrive" + str(diskNo)
        hfile = win32file.CreateFile( drivename, win32con.GENERIC_READ| ntsecuritycon.FILE_READ_ATTRIBUTES , win32con. FILE_SHARE_READ, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        self.__channel = EHUploadChannel.EHUploadChannel('44110126-f7fd-477e-bd9b-9a95ec88419c' , self.__key, self.__secret, 71*1024*1024*1024 , 'sat-p' , 'testuploadwrk')
        
        dataplace = 0
        mb = 1024*1024*16
        (result , data) = win32file.ReadFile(hfile,mb,None)
        dataext = DataExtent.DataExtent(dataplace , len(data))
        dataext.setData(data)
        dataplace = dataplace + len(data)
        self.__channel.uploadData(dataext)
        
        self.__channel.waitTillUploadComplete()    
        diskid = self.__channel.confirm()
        logging.info("Disk "+ diskid+ " was uploaded!");

    def test_workDiskUpload(self):
        logging.info("test disabled");
        return 
        diskNo = 5
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        drivename = "\\\\.\\PhysicalDrive" + str(diskNo)
        hfile = win32file.CreateFile( drivename, win32con.GENERIC_READ| ntsecuritycon.FILE_READ_ATTRIBUTES , win32con. FILE_SHARE_READ, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        self.__channel = EHUploadChannel.EHUploadChannel('' , self.__key, self.__secret, 90*1024*1024*1024 , 'sat-p' , 'testuploadwrk')
        

        dataplace = 0
        while 1:
            mb = 1024*1024
            try:
                (result , data) = win32file.ReadFile(hfile,mb,None)
            except:
                break
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataext.setData(data)
            dataplace = dataplace + len(data)
            self.__channel.uploadData(dataext)
        
        self.__channel.waitTillUploadComplete()    
        diskid = self.__channel.confirm()
        logging.info("Disk "+ diskid+ " was uploaded!");

if __name__ == '__main__':
    unittest.main()
