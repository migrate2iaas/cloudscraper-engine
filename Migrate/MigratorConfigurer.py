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

        #NOTE: only the system info is needed here!
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

        imagearch = config.get('Image', 'source-arch')

        if config.has_option('Image', 'image-type'):
            imagetype = config.get('Image', 'image-type')
        

        imagedir = ""
        if config.has_option('Image', 'image-dir'):
           imagedir = config.get('Image', 'image-dir') 
           
        volumes = list()
        
        if config.has_section('Volumes') :
            if config.has_option('Volumes', 'letters'):
                letters = config.get('Volumes', 'letters');
                for letter in letters.split(','):
                    devicepath = '\\\\.\\'+letter+':';
                    size = Windows.Windows().getSystemInfo().getVolumeInfo(letter+":").getSize()
                    volumes.append( (devicepath , imagedir+"\\"+letter+"."+imagetype, size ) )
                    #TODO: better to create suing [Volume_Letter] section + optionsSize here then!
                
        else:    
            #the old default version of initializing
            if imagedir:
                imagepath = imagedir+"\\system."+imagetype
            if imagepath == '':
                imagepath = config.get('Image', 'image-path')
            try:
                imagesize = config.getint('Image' , 'image-size')
                newsize = config.get('EC2', 'instance-size')
            except ConfigParser.NoOptionError as exception:
                logging.info("No image type or size found, using the default ones");
                newsize = imagesize
            volumes.append( (Windows.Windows().getSystemInfo().getSystemVolumeInfo().getDevicePath() , imagepath , imagesize ))
        


        try:
            bucket = config.get('EC2', 'bucket')
        except ConfigParser.NoOptionError as exception:
            logging.info("No bucket name found, generating a new one")
            #TODO: move to a function
            bucket = "cloudscraper-" + str(int(time.mktime(time.localtime())))+"-"+region
             

        newsize = imagesize
        adjust = AmazonConfigs.AmazonAdjustOptions()
        image = AmazonConfigs.AmazonMigrateConfig(volumes , imagearch , imagetype)
        cloud = AmazonConfigs.AmazonCloudOptions(bucket , user , password , newsize , arch , zone , region)

        return (image,adjust,cloud)

        #TODO: need kinda config validator!
