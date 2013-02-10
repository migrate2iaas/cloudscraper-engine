import AmazonConfigs

import EC2InstanceGenerator
import EC2Instance

import ConfigParser
import logging
import time

class MigratorConfigurer(object):
    """ This class is up to make configs for various cloud migrations"""

    def __init__(self):
        return


    #returns the tuple containing the config info (Image,Fixups,Cloud)
    def configAmazon(self , configfile , user = '' , password = '' , region = '', imagepath = ''):
        
        config = ConfigParser.RawConfigParser()
        config.read(configfile)

        import Windows
        imagesize = Windows.Windows().getSystemInfo().getSystemVolumeInfo().getSize()
        #TODO: checks OS
        imagetype = 'VHD'
      

        #cloud config
        if user == '':
            user = config.get('EC2', 's3key')
        if password == '':
            password = config.get('EC2', 's3secret')
        
        intancetype = config.get('EC2', 'instance-type')

        if region == '':
            region = config.get('EC2', 'region')

        try:
            zone = config.get('EC2', 'zone')
        except ConfigParser.NoOptionError as exception:
            zone = region+"a"

        #TODO: more amazon-specfiic configs needed
        # TODO: what to do with the arch
        arch = config.get('EC2', 'target-arch')
        #TODO: check it!

        imagearch = config.get('Image', 'source-arch')

        if imagepath == '':
            imagepath = config.get('Image', 'image-path')
        
        try:
            imagetype = config.get('Image', 'image-type')
            imagesize = config.getint('Image' , 'image-size')
            newsize = config.get('EC2', 'instance-size')
            #TODO: check the size
        except ConfigParser.NoOptionError as exception:
            logging.info("No image type or size found, using the default ones");
            newsize = imagesize

        try:
            bucket = config.get('EC2', 'bucket')
        except ConfigParser.NoOptionError as exception:
            logging.info("No bucket name found, generating a new one")
            #TODO: move to a function
            bucket = "migrate-" + str(int(time.mktime(time.localtime())))+"-"+region
             

        newsize = imagesize
        adjust = AmazonConfigs.AmazonAdjustOptions()
        image = AmazonConfigs.AmazonMigrateConfig(imagepath , imagesize  , imagearch , imagetype)
        cloud = AmazonConfigs.AmazonCloudOptions(bucket , user , password , newsize , arch , zone , region)

        return (image,adjust,cloud)

        #TODO: need kinda config validator!
