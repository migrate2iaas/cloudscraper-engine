# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import AmazonConfigs
import EHConfigs

import EC2InstanceGenerator
import EC2Instance
import os
import stat
import ConfigParser
import logging
import time
from MigrateConfig import VolumeMigrateConfig

class VolumeMigrateNoConfig(VolumeMigrateConfig):
    def __init__(self, volumename, imagepath , imagesize):
        self.__volumeName = volumename
        self.__imagePath = imagepath
        self.__imageSize = imagesize
        self.__pathToUpload = ''
        self.__uploadedImageId = ''

    def getImagePath(self):
        return self.__imagePath

    def getUploadPath(self):
        return  self.__pathToUpload;

    def getUploadId(self):
        return self.__uploadedImageId

    def getImageSize(self):
        return self.__imageSize

    def getVolumePath(self):
        return self.__volumeName

    def setUploadPath(self, path):
        self.__pathToUpload = path

    def setUploadId(self , uploadid):
        self.__uploadedImageId = uploadid

    def setImagePath(self , imagepath):
        self.__imagePath = imagepath

    def setImageSize(self , size):
        self.__imageSize = size
   
    def saveConfig(self):
        return

class VolumeMigrateIniConfig(VolumeMigrateConfig):
    """ volume migration parms got from ini file """

    #NOTE: really , there are just two functions to override: load config and save config
    # the common code should be moved to base class then
    def __init__(self, config, configname , section, volumename):
        self.__config = config
        self.__section = section
        self.__configFileName = configname
        self.__volumeName = volumename
        self.__imagePath = ''
        self.__imageSize = 0
        self.__pathToUpload = ''
        self.__uploadedImageId = ''
        if config.has_section(section):
            if config.has_option(section, 'imagesize'):
                self.__imageSize = config.getint(section, 'imagesize')

            if config.has_option(section, 'pathuploaded'):
                self.__uploadedImageId = config.get(section, 'pathuploaded')

            if config.has_option(section, 'pathtoupload'):
                self.__pathToUpload = config.get(section, 'pathtoupload')

            if config.has_option(section, 'imagepath'):
                self.__imagePath = config.get(section, 'imagepath')
        else:
            logging.error("!!!ERROR: bad config file. Section for drive letter cannot be found");
            raise LookupError


    def getImagePath(self):
        return self.__imagePath

    def getUploadPath(self):
        return  self.__pathToUpload;

    def getUploadId(self):
        return self.__uploadedImageId

    def getImageSize(self):
        return self.__imageSize

    def getVolumePath(self):
        return self.__volumeName

    def setUploadPath(self, path):
        self.__pathToUpload = path

    def setUploadId(self , uploadid):
        self.__uploadedImageId = uploadid

    def setImagePath(self , imagepath):
        self.__imagePath = imagepath
   
    def setImageSize(self , size):
        self.__imageSize = size

    def saveConfig(self):
        section = self.__section
        if self.__config.has_section(section) == False:
            self.__config.add_section(section)
        
        if self.__imageSize:
            self.__config.set(section, 'imagesize' , str(self.__imageSize))

        if self.__uploadedImageId:
            self.__config.set(section, 'pathuploaded' , self.__uploadedImageId)

        if self.__pathToUpload:
            self.__config.set(section, 'pathtoupload' , self.__pathToUpload)

        if self.__imagePath:
           self.__config.set(section, 'imagepath', self.__imagePath)

        fconf = file(self.__configFileName, "w")
        self.__config.write(fconf)


class MigratorConfigurer(object):
    """ This class is up to make configs for various cloud migrations"""

    def __init__(self):
        return

    #automcatically chooses which cloud to generate the config for
    def configAuto(self , configfile, password = ''):
        config = ConfigParser.RawConfigParser()
        config.read(configfile)
        if config.has_section('EC2'):
            return configAmazon(configfile , '' , password)
        if config.has_section('ElasticHosts'):
            return configElasticHosts(configfile , '' , password)
        return None

    #returns the tuple containing the config info (Image,Fixups,Cloud)
    def configAmazon(self , configfile , user = '' , password = '' , region = '', imagepath = ''):
        
        config = ConfigParser.RawConfigParser()
        config.read(configfile)

        import Windows
        imagesize = Windows.Windows().getSystemInfo().getSystemVolumeInfo().getSize() 
        imagetype = 'VHD'      

        #cloud config
        if user == '':
            user = config.get('EC2', 's3key')
        if password == '':
            password = config.get('EC2', 's3secret')
        
        intancetype = config.get('EC2', 'instance-type')

        if region == '':
            region = config.get('EC2', 'region')

        arch = config.get('EC2', 'target-arch')

        imagearch = config.get('Image', 'source-arch')

        if config.has_option('Image', 'image-type'):
            imagetype = config.get('Image', 'image-type')
        else:
            imagetype = 'VHD'
            logging.warning("No image type specified. Default VHD is used.");
        

        imagedir = ""
        if config.has_option('Image', 'image-dir'):
           imagedir = config.get('Image', 'image-dir') 
        else:
            imagedir = "."
            logging.warning("No directory for image store is specified. It'll be created in local script execution directory");

        if imagedir[-1] == '\\':
            imagedir = imagedir[0:-1]
        if os.path.exists(imagedir) == False:
            logging.debug("Directory " + str(imagedir) + " not found, creating it");
            os.mkdir(imagedir)           
        else:
            dirmode = os.stat(imagedir).st_mode
            if stat.S_ISDIR(dirmode) == False:
                #TODO: create wrapper for error messages
                #TODO: test UNC path
                logging.error("!!!ERROR Directory given for image storage is not valid!");

            
        s3prefix = ""
        if config.has_option('EC2', 's3prefix'):
           s3prefix = config.get('EC2', 's3prefix') 
       
        
        try:
            zone = config.get('EC2', 'zone')
        except ConfigParser.NoOptionError as exception:
            zone = region+"a"
        
       
        bucket = ''

        try:
            bucket = config.get('EC2', 'bucket')
        except ConfigParser.NoOptionError as exception:
            logging.info("No bucket name found, generating a new one")

        if bucket == '':
            #TODO: make another time mark: minutes-seconds-machine-name?
            bucket = "cloudscraper-" + str(int(time.mktime(time.localtime())))+"-"+region 
            #NOTE: it'll be saved on the next ocassion
            config.set('EC2', 'bucket' , bucket)
            #TODO: the next ocassion is somewhere after the imaging passed 

        
        security = "default"
        try:
            security = config.get('EC2', 'security-group')
        except ConfigParser.NoOptionError as exception:
            logging.info("No security group was speicified, using default one. Note it couldn't be changed afterwards.")

        volumes = list()
        if config.has_section('Volumes') :
            if config.has_option('Volumes', 'letters'):
                letters = config.get('Volumes', 'letters');
                for letter in letters.split(','):
                    devicepath = '\\\\.\\'+letter+':';
                    size = Windows.Windows().getSystemInfo().getVolumeInfo(letter+":").getSize()
                    volume = VolumeMigrateIniConfig(config , configfile , letter , devicepath)
                    if volume.getImagePath() == '':
                        volume.setImagePath(imagedir+"\\"+letter+"."+imagetype);
                    if volume.getImageSize() == 0:
                        volume.setImageSize(size);
                    s3objkey = letter
                    if s3prefix:
                        s3objkey = s3prefix+"/"+s3objkey
                    if volume.getUploadPath() == '':
                        volume.setUploadPath(s3objkey)
                    volumes.append( volume )
                
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
            volumes.append( VolumeMigrateNoConfig(Windows.Windows().getSystemInfo().getSystemVolumeInfo().getDevicePath() , imagepath , imagesize ))
        

        newsize = imagesize
        adjust = AmazonConfigs.AmazonAdjustOptions()
        image = AmazonConfigs.AmazonMigrateConfig(volumes , imagearch , imagetype)
        cloud = AmazonConfigs.AmazonCloudOptions(bucket , user , password , newsize , arch , zone , region, security)

        return (image,adjust,cloud)

    #note its better
    def configElasticHosts(self , configfile , user = '' , password = '' , region = '', imagepath = ''):
        config = ConfigParser.RawConfigParser()
        config.read(configfile)

        import Windows
        imagesize = Windows.Windows().getSystemInfo().getSystemVolumeInfo().getSize()
        imagetype = 'VHD'      

        #cloud config
        if user == '':
            user = config.get('ElasticHosts', 'user-uuid')
        if password == '':
            password = config.get('ElasticHosts', 'ehsecret')
        
        if region == '':
            region = config.get('ElasticHosts', 'region')

        #really, it doesn't matter for the EH cloud
        imagearch = config.get('Image', 'source-arch')

        if config.has_option('Image', 'image-type'):
            imagetype = config.get('Image', 'image-type')
        else:
            imagetype = 'raw.gz'
            logging.warning("No image type specified. Default raw.gz is used.");
        
        imagedir = ""
        if config.has_option('Image', 'image-dir'):
           imagedir = config.get('Image', 'image-dir') 
        else:
            imagedir = "."
            logging.warning("No directory for image store is specified. It'll be created in local script execution directory");

        if imagedir[-1] == '\\':
            imagedir = imagedir[0:-1]
        if os.path.exists(imagedir) == False:
            logging.debug("Directory " + str(imagedir) + " not found, creating it");
            os.mkdir(imagedir)           
        else:
            dirmode = os.stat(imagedir).st_mode
            if stat.S_ISDIR(dirmode) == False:
                #TODO: create wrapper for error messages
                #TODO: test UNC path
                logging.error("!!!ERROR Directory given for image storage is not valid!");

        image_placement = ""
        if config.has_option('Image', 'image-placement'):
           image_placement = config.get('Image', 'image-placement') 
        else:
            image_placement = "local"

        volumes = list()
        if config.has_section('Volumes') :
            if config.has_option('Volumes', 'letters'):
                letters = config.get('Volumes', 'letters');
                for letter in letters.split(','):
                    devicepath = '\\\\.\\'+letter+':';
                    size = Windows.Windows().getSystemInfo().getVolumeInfo(letter+":").getSize()
                    volume = VolumeMigrateIniConfig(config , configfile , letter , devicepath)
                    if volume.getImagePath() == '':
                        volume.setImagePath(imagedir+"\\"+letter+"."+imagetype);
                    if volume.getImageSize() == 0:
                        volume.setImageSize(size);
                    volumes.append( volume )
        
        newsize = imagesize
        adjust = EHConfigs.EHAdjustOptions()
        image = EHConfigs.EHMigrateConfig(volumes , imagearch , image_placement, imagetype)
        cloud = EHConfigs.EHCloudOptions( user , password , newsize , imagearch , region)

        return (image,adjust,cloud)