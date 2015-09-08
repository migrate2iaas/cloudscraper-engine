# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys
# should replace it with more python-like stuff like having __init__.py for each subdir and importing it
sys.path.append('./Amazon')
sys.path.append('./ElasticHosts')
sys.path.append('./Azure')
sys.path.append('./CloudSigma')
sys.path.append('./SelfTest')
sys.path.append('./Images')
sys.path.append('./ProfitBricks')
sys.path.append('./onApp')
sys.path.append('./MiniPad')

import unittest

import AdjustedBackupSource
import BackupAdjust
import Migrator
import SystemAdjustOptions

import getpass
import platform

import logging
import MigratorConfigurer
import datetime
import traceback
import argparse
import time
import traceback
import Version
import random
import errno
import threading 
import os
import md5
import sha
import base64


def encode(key, clear):
    enc = []
    for i in range(len(clear)):
        key_c = key[i % len(key)]
        enc_c = chr((ord(clear[i]) + ord(key_c)) % 256)
        enc.append(enc_c)
    return base64.urlsafe_b64encode("".join(enc))

def decode(key, enc):
    dec = []
    enc = base64.urlsafe_b64decode(enc)
    for i in range(len(enc)):
        key_c = key[i % len(key)]
        dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)

jan1 = str(int(time.mktime((2016,1,1,1,1,1,1,1,1))))
encoded = encode("0xEDAEDA", jan1)
print (encoded)
decoded = decode("0xEDAEDA", encoded)
print (decoded)
print (time.ctime(int(decoded)))

if os.name == 'nt':
    sys.path.append('./Windows')
    import WindowsVolume
    import WindowsBackupSource

MigrateVerisonHigh = Version.majorVersion
MigrateVersionLow = Version.minorVersion


def heartbeat(interval_sec):
    while 1:
        logging.info(".")
        print('.')
        time.sleep(int(interval_sec))


def get_pkk():
    """gets packages"""
    # gets private key to decode licensing
    # it's so ugly in case to be secret, Arrrgghh!
    return "0xEDAEDA"

def chk_ver():
    """checks version"""
    #checks the license file, returns 
    #it's so ugly in case to be secret
    curdir = os.path.dirname(os.path.realpath(__file__))
    license_path = curdir + "/../../lcns.msg"
    if os.path.exists(license_path): 
        file = open(license_path , "r")
        data = file.readline()
        pkk = get_pkk()
        try:
            decoded = decode(pkk, data)
            expiration = long(decoded)
            if long(time.time()) > expiration:
                logging.info(">>> The license key is expired")
                return False
        except Exception as e:
            logging.error("!!!ERROR: Error occured while reading the licensing info, please visit www.migarte2iaas.com to obtain new license")
            raise e
        file.close()
        return True
    else:
        return False

def chk_limits():
    if chk_ver() == False:
        logging.info(">>> The version is free but limited")
        logging.info(">>> The limit is 20 GB data transfered")
        logging.info(">>> Please visit www.migrate2iaas.com to obtain license")
        return long(20*1024*1024*1024)
    return 0

if __name__ == '__main__':
    try:
        #Turning on the logging
        logging.basicConfig(format='%(asctime)s %(message)s' , filename='../../logs/migrate.log',level=logging.DEBUG)    
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(handler)

        # little hacks to pre-configure env and args
        if os.name == 'nt':
            import Windows
            #converting to unicode, add "CheckWindows" option
            sys.argv = Windows.win32_unicode_argv()
        else:
            import Linux
            #making it windows-compatible, should refactor then
            os.environ["windir"]=Linux.Linux().getSystemDriveName().replace("/dev/" , "")
            os.environ["COMPUTERNAME"]=os.uname()[1]

        #parsing extra option
        parser = argparse.ArgumentParser(description="This script performs creation of virtualized images from the local server, uploading them to S3, converting them to EC2 instances. See http://www.migrate2iaas.com for more details.")
        parser.add_argument('-k', '--amazonkey', help="Your AWS secret key. ")
        parser.add_argument('-e', '--ehkey', help="Your ElasicHosts API secret key.")
        parser.add_argument('-i', '--cloudsigmapass', help="Your CloudSigma password.")
        parser.add_argument('-a', '--azurekey', help="Your Azure storage account primary key.")
        parser.add_argument('-w', '--apikey', help="Your generic API key: the cloud will be choosen according to the config file.")
        parser.add_argument('-c', '--config', help="Path to copy config file")
        parser.add_argument('-o', '--output', help="Path to extra file for non-detalized output")                   
        parser.add_argument('-u', '--resumeupload', help="Resumes the upload of image already created", action="store_true")                   
        parser.add_argument('-s', '--skipupload', help="Skips both imaging and upload. Just start the machine in cloud from the image given", action="store_true")                   
        parser.add_argument('-t', '--testrun', help="Makes test run on the migrated server to see it responding.", action="store_true") 
        parser.add_argument('-z', '--timeout', help="Specify timeout to wait for test run server to respond", type=int, default=480)                  
        parser.add_argument('-b', '--heartbeat', help="Specifies interval in seconds to write hearbeat messages to stdout. No heartbeat if this flag is ommited", type=int)                   
        parser.add_argument('-q', '--statusfile', help="Specifies status file to write current output") 
        parser.add_argument('-v', '--virtio', help="Injects virtio drivers in the running server driver store", action="store_true")
        parser.add_argument('-j', '--reboottimeout', help="Time to wait in seconds for VM to reboot while doing test run", type=int, default=200)
    
        #new random seed
        random.seed()
    
        #some legacy command-line support
        if parser.parse_args().output:
            outhandler = logging.FileHandler(parser.parse_args().output , "w" )
            outhandler.setLevel(logging.INFO)
            logging.getLogger().addHandler(outhandler)
    
        # starting the heartbeat thread printing some dots while app works
        if parser.parse_args().heartbeat:
            threading.Thread(target = heartbeat, args=(parser.parse_args().heartbeat,) ).start()

        logging.info("\n>>>>>>>>>>>>>>>>> The Server Transfer Process ("+ Version.getShortVersionString() + ") is initializing\n")
        logging.info("Full version: " + Version.getFullVersionString())


        config = MigratorConfigurer.MigratorConfigurer()
        # creatiing the config
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
        azurekey=''
        cloudsigmapass=''
        apikey = ''
        if parser.parse_args().ehkey:
            ehkey = parser.parse_args().ehkey
        if parser.parse_args().amazonkey:
            s3key = parser.parse_args().amazonkey    
        if parser.parse_args().azurekey:
            azurekey = parser.parse_args().azurekey    
        if parser.parse_args().cloudsigmapass:
            cloudsigmapass = parser.parse_args().cloudsigmapass    
        if parser.parse_args().apikey:
            apikey = parser.parse_args().apikey  

        testrun = False
        timeout = 0
        reboottimeout = 0
        if parser.parse_args().testrun:
            testrun = True
            timeout = parser.parse_args().timeout
            reboottimeout = parser.parse_args().resumeupload

        resumeupload = False
        if parser.parse_args().resumeupload:
            resumeupload = True
        
        skipupload = False
        if parser.parse_args().skipupload:
            skipupload = True
            #NOTE: skip upload is not yet good enough! 
            # it needs saving of 1) image-id (xml-key) for each volume imported previously 2) instance-id

        password = s3key or ehkey or azurekey or cloudsigmapass or apikey
        limits = None
        try:
            #configuring the process
            (image,adjust,cloud) = config.configAuto(configpath , password)
            limits = chk_limits()

        except Exception as e:
            logging.error("\n!!!ERROR: failed to configurate the process! ")
            logging.error("\n!!!ERROR: " + repr(e) )
            logging.error(traceback.format_exc())
            os._exit(errno.EFAULT)
    
        logging.info("\n>>>>>>>>>>>>>>>>> Configuring the Transfer Process:\n")
        __migrator = Migrator.Migrator(cloud,image,adjust, resumeupload or skipupload , resumeupload, skipupload , limits = limits , insert_vitio=parser.parse_args().resumeupload)
        logging.info("Migrator test started")
        # Doing the task
        instance = None
        try:
            instance = __migrator.runFullScenario()
            if instance:
                logging.info("\n>>>>>>>>>>>>>>>>> The server is in the stopped state, run it via " + str(cloud.getTargetCloud()) + " management console\n")
                logging.info("\n>>>>>>>>>>>>>>>>> Transfer process ended successfully\n")
        except Exception as e:
            logging.error("\n!!!ERROR: Severe error while making the migration")
            logging.error("\n!!!ERROR: " + str(e) )
            logging.error(traceback.format_exc())

        if not instance:
               logging.info("\n>>>>>>>>>>>>>>>>>> Transfer process ended unsuccessfully\n")
               #sys.exit(errno.EFAULT)
               os._exit(errno.EFAULT)

        # check if server responds in the test scenario
        # TODO: move the code to spearate file\class, looks ugly
        try:
            if testrun:
                logging.info("\n>>>>>>>>>>>>>>>>> Making test run for an instance to check it alive\n")
                instance.stop()
                logging.info("Waiting a bit for server restart")
                time.sleep(reboottimeout)
                instance.run()
                logging.info("\n>>>>>>>>>>>>>>>>> Waiting till it responds\n")
                port = 22
                if os.name == 'nt':
                    port = 3389
                response = instance.checkAlive(timeout , port)
                if response:
                    logging.info("\n>>>>>>>>>>>>>>>>> Transfer post-check ended successfully\n")
                else:
                    logging.error("!!!ERROR: Transfer process post-check ended unsuccessfully for " + str(instance) + " at " + str(cloud.getTargetCloud()) + " , " + str(cloud.getRegion()));
        except Exception as e:
            logging.error("\n!!!ERROR: failed tomake a test check! ")
            logging.error("\n!!!ERROR: " + str(e) )
            logging.error(traceback.format_exc())
            logging.info("\n!!!ERROR: Transfer process post-check ended unsuccessfully\n")
            os._exit(errno.ERANGE)
            #sys.exit(errno.ERANGE)
        finally:
            try:
                instance.stop()
            except Exception as e:
                logging.warning("!Cannot stop the instance: " + e.str())

        os._exit(0)
    except Exception as e:
         logging.error("\n!!!ERROR: Unexpected error during init")
         logging.error("\n!!!ERROR: " + str(e) )
         logging.error(traceback.format_exc())
         logging.info("\n!!!ERROR: Transfer process ended unsuccessfully\n")
         os._exit(errno.ERANGE)
    finally:
        os._exit(0)