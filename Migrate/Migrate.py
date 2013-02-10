

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


#TODO: make versioning and expiration time!!

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s %(message)s' , filename='..\\..\\logs\\migrate.log',level=logging.DEBUG)    
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)
    
    # TODO: move all amazon configuration prompts to the interactive Configurer class

    logging.info("\n>>>>>>>>>>>>>>>>> Configuring the Migration Process:\n")

    config = MigratorConfigurer.MigratorConfigurer()

    logging.info("\n>>>>>>>>>>>>>>>>> Your Amazon EC2 Account:")
    
    s3owner = raw_input("Your Access Key ID:")
    s3key = getpass.getpass("Secret Access Key:")

    logging.info("\n>>>>>>>>>>>>>>>>> Imaging options")

    imagepath = raw_input("Enter file path to store the server image:")

    #TODO: Make parm checks in migrator
    region = ''

    (image,adjust,cloud) = config.configAmazon("..\\..\\cfg\\amazon.ini" , s3owner , s3key , region , imagepath)
    #TODO: here we must adjust some of pregenerated values? 
    #TODO: or just make the other parts know whole the stuff and configs???
    #TODO: check if the image file is already created. do we need to skip the backup then? 
    # or what? Think on pre-generated vhds matters...
    
    logging.info("\n>>>>>>>>>>>>>>>>> Starting the Migration Process:\n")

    __migrator = Migrator.Migrator(cloud,image,adjust)
    logging.info("Migrator test started")
    __migrator.runFullScenario()
    logging.info("Migrator test ended")

    


