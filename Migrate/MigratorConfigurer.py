import AmazonConfigs

import EC2InstanceGenerator
import EC2Instance

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

       

        # TODO: what to do with the arch
        # TODO: security groups handling is needed too
        arch = config.get('EC2', 'target-arch')

        imagearch = config.get('Image', 'source-arch')

        if config.has_option('Image', 'image-type'):
            imagetype = config.get('Image', 'image-type')
        
        #TODO: check for config validity and raise exceptions if it's not good enough
        imagedir = ""
        if config.has_option('Image', 'image-dir'):
           imagedir = config.get('Image', 'image-dir') 

        #TODO: buggy stuff. make the check better
        if imagedir[-1] == '\\':
            imagedir = imagedir[0:-2]

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
            bucket = "cloudscraper-" + str(int(time.mktime(time.localtime())))+"-"+region 
            #NOTE: it'll be saved on the next ocassion
            config.set('EC2', 'bucket' , bucket)
            #TODO: the next ocassion is somewhere after the imaging passed 

        volumes = list()
        #TODO: image-dir is obligatory here!
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
        cloud = AmazonConfigs.AmazonCloudOptions(bucket , user , password , newsize , arch , zone , region)

        return (image,adjust,cloud)

        #TODO: need kinda config validator!
