# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('.\Windows')
sys.path.append('.\Amazon')
sys.path.append('.\ElasticHosts')

import unittest
import WindowsVolume
import WindowsBackupSource
import AdjustedBackupSource
import BackupAdjust
import Migrator
import SystemAdjustOptions

import getpass

import Windows

import logging
import MigratorConfigurer
import datetime
import traceback
import argparse
import sysconfig
import time

MigrateVerisonHigh = 0
MigrateVersionLow = 3


#TODO: make versioning and expiration time!!

if __name__ == '__main__':
    
    #parsing extra option
    parser = argparse.ArgumentParser(description="This script performs creation of virtualized images from the local server, uploading them to S3, converting them to EC2 instances. See README for more details.")
    parser.add_argument('-k', '--amazonkey', help="Your AWS secret key. If not specified you will be prompted at the script start")
    parser.add_argument('-e', '--ehkey', help="Your ElasicHosts API secret key. If not specified you will be prompted at the script start")
    parser.add_argument('-c', '--config', help="Path to server cloud copy config file")
    parser.add_argument('-o', '--output', help="Path to extra file for non-detalized output")                   
    parser.add_argument('-u', '--resumeupload', help="Resumes the upload of image already created", action="store_true")                   
    parser.add_argument('-s', '--skipupload', help="Skips both imaging and upload. Just start the machine in cloud from the image given", action="store_true")                   

    #Turning on the logging
    logging.basicConfig(format='%(asctime)s %(message)s' , filename='..\\..\\logs\\migrate.log',level=logging.DEBUG)    
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)
    
    if parser.parse_args().output:
        outhandler = logging.FileHandler(parser.parse_args().output , "w" )
        outhandler.setLevel(logging.INFO)
        logging.getLogger().addHandler(outhandler)
    
    logging.info("\n>>>>>>>>>>>>>>>>> The Server Transfer Process (v " + str(MigrateVerisonHigh)+ "." + str(MigrateVersionLow) +  ") is initializing\n")

    config = MigratorConfigurer.MigratorConfigurer()
    
    if parser.parse_args().config:
        configpath = parser.parse_args().config
        s3owner = ''
        imagepath = ''
        region = ''
    else:
        configpath = "..\\..\\cfg\\amazon.ini"
        s3owner = raw_input("Your Access Key ID:")
        imagepath = raw_input("Enter file path to store the server image:")
        region = ''

    s3key=''
    ehkey=''
    if parser.parse_args().ehkey:
        ehkey = parser.parse_args().ehkey
    else:
        if parser.parse_args().amazonkey:
            s3key = parser.parse_args().amazonkey    
        else:
            s3key = getpass.getpass("AWS Secret Access Key:")



    resumeupload = False
    if parser.parse_args().resumeupload:
        resumeupload = True
        
    skipupload = False
    if parser.parse_args().skipupload:
        skipupload = True
        #NOTE: skip upload is not yet good enough! 
        # it needs saving of 1) image-id (xml-key) for each volume imported previously 2) instance-id

    #thus we use Amazon
    try:
        if s3key:
            (image,adjust,cloud) = config.configAmazon(configpath , s3owner , s3key , region , imagepath)
        if ehkey:
            (image,adjust,cloud) = config.configElasticHosts(configpath , '' , ehkey) 
    except Exception as e:
        logging.error("\n!!!ERROR: failed to configurate the process! ")
        logging.error("\n!!!ERROR: " + str(e) )
    
    logging.info("\n>>>>>>>>>>>>>>>>> Configuring the Transfer Process:\n")
    __migrator = Migrator.Migrator(cloud,image,adjust, resumeupload or skipupload , resumeupload, skipupload)
    logging.info("Migrator test started")
    __migrator.runFullScenario()
    logging.info("\n>>>>>>>>>>>>>>>>> Transfer process ended\n")

    


