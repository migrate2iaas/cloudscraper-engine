

import sys

sys.path.append('.\Windows')
sys.path.append('.\Amazon')

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

MigrateVerisonHigh = 0
MigrateVersionLow = 1


#TODO: make versioning and expiration time!!

if __name__ == '__main__':

    
    #parsing extra option
    parser = argparse.ArgumentParser(description="This script performs creation of virtualized images from the local server, uploading them to S3, converting them to EC2 instances. See README for more details.")
    parser.add_argument('-k', '--amazonkey', help="Your AWS secret key. If not specified you will be prompted at the script start")
    parser.add_argument('-c', '--config', help="Path to server cloud copy config file")
    parser.add_argument('-o', '--output', help="Path to extra file for non-detalized output")                   

    #Turning on the logging
    logging.basicConfig(format='%(asctime)s %(message)s' , filename='..\\..\\logs\\migrate.log',level=logging.DEBUG)    
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)
    
    if parser.parse_args().output:
        outhandler = logging.FileHandler(parser.parse_args().output , "w" )
        outhandler.setLevel(logging.INFO)
        logging.getLogger().addHandler(outhandler)
    
    logging.info("\n>>>>>>>>>>>>>>>>> Configuring the Migration Process (v " + str(MigrateVerisonHigh)+ "." + str(MigrateVersionLow) +  "):\n")

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

    if parser.parse_args().amazonkey:
        s3key = parser.parse_args().amazonkey    
    else:
        s3key = getpass.getpass("AWS Secret Access Key:")

    (image,adjust,cloud) = config.configAmazon(configpath , s3owner , s3key , region , imagepath)
    
    logging.info("\n>>>>>>>>>>>>>>>>> Starting the Migration Process:\n")
    __migrator = Migrator.Migrator(cloud,image,adjust)
    logging.info("Migrator test started")
    __migrator.runFullScenario()
    logging.info("Migrator test ended")

    


