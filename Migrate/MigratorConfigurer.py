# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('./..')
sys.path.append('./../Helper')
sys.path.append('./Helper')


import AmazonConfigs
import EHConfigs
import AzureConfigs
import CloudSigmaConfigs

import platform
import shutil

import EC2InstanceGenerator
import EC2Instance
import os
import stat
import ConfigParser
import logging
import codecs
import time
from MigrateConfig import VolumeMigrateConfig
import UnicodeConfigParser
import GzipChunkMedia
import GzipChunkMediaFactory
import RawGzipMediaFactory
import datetime
import StreamVmdkMediaFactory
import ProfitBricksConfig

class VolumeMigrateNoConfig(VolumeMigrateConfig):
    def __init__(self, volumename, imagepath , imagesize):
        self.__volumeName = volumename
        self.__imagePath = imagepath
        self.__imageSize = imagesize
        self.__pathToUpload = ''
        self.__uploadedImageId = ''
        self.__excludedDirs = list()

    def getImagePath(self):
        return self.__imagePath

    def getUploadPath(self):
        return  self.__pathToUpload 

    def getUploadId(self):
        return self.__uploadedImageId

    def getImageSize(self):
        return self.__imageSize

    def getVolumePath(self):
        return self.__volumeName

    def getExcludedDirs(self):
        return self.__excludedDirs

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

    def generateMigrationId(self):
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
        self.__excludedDirs = list()

        if config.has_section(section):
            if config.has_option(section, 'imagesize'):
                self.__imageSize = config.getint(section, 'imagesize')
            else:
                logging.debug("imagesize was not found in the config for volume " + str(self.__volumeName)) 

            if config.has_option(section, 'pathuploaded'):
                self.__uploadedImageId = config.get(section, 'pathuploaded')
            else:
                logging.debug("pathuploaded was not found in the config for volume " + str(self.__volumeName)) 

            if config.has_option(section, 'pathtoupload'):
                self.__pathToUpload = config.get(section, 'pathtoupload')
            else:
                logging.debug("pathtoupload was not found in the config for volume " + str(self.__volumeName)) 

            if config.has_option(section, 'imagepath'):
                self.__imagePath = config.get(section, 'imagepath')
            else:
                logging.debug("imagepath was not found in the config for volume " + str(self.__volumeName)) 

            # excludedir is a string of dirs separated by ;
            if config.has_option(section, 'excludedir'):
                dirstr = config.get(section, 'excludedir')
                self.__excludedDirs = dirstr.split(";")
            else:
                logging.debug("excludedir was not found in the config for volume " + str(self.__volumeName)) 
        else:
            logging.error("!!!ERROR: bad config file. Section for drive letter cannot be found") 
            raise LookupError


    def getImagePath(self):
        return self.__imagePath

    def getUploadPath(self):
        return  self.__pathToUpload 

    def getUploadId(self):
        return self.__uploadedImageId

    def getImageSize(self):
        return self.__imageSize

    def getVolumePath(self):
        return self.__volumeName

    def getExcludedDirs(self):
        return self.__excludedDirs

    def setUploadPath(self, path):
        self.__pathToUpload = path

    def setUploadId(self , uploadid):
        self.__uploadedImageId = uploadid

    def setImagePath(self , imagepath):
        self.__imagePath = imagepath
   
    # image size here is the size of volume in bytes (not in the image file that could be compressed)
    def setImageSize(self , size):
        self.__imageSize = size

    def generateMigrationId(self):
        """generates an id to distinguish migration of the same volumes but for different times"""
        return (os.environ["COMPUTERNAME"] + "_" + datetime.date.today().strftime("%Y_%m_%d") + "_" + str(self.getVolumePath())).replace("\\" , "").replace("." , "_").replace(":" , "")

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

        fconf = codecs.open(self.__configFileName, "w", "utf16")#file(self.__configFileName, "w")
        self.__config.write(fconf)


class MigratorConfigurer(object):
    """ This class is up to make configs for various cloud migrations"""

    def __init__(self):
        return

    #automcatically chooses which cloud to generate the config for
    def configAuto(self , configfile, password = ''):
        try:
            config = UnicodeConfigParser.UnicodeConfigParser()
            config.readfp(codecs.open(configfile, "r", "utf16"))
        except Exception as e:
            logging.info("Couldn't read an config as unicode file due to " + str(e) + " . Reading as ascii")
            config = ConfigParser.RawConfigParser()
            config.read(configfile)
        if config.has_section('EC2'):
            return self.configAmazon(configfile , '' , password)
        if config.has_section('ElasticHosts'):
            return self.configElasticHosts(configfile , '' , password)

        if config.has_section('Azure'):
            return self.configAzure(configfile , config , password)

        if config.has_section('CloudSigma'):
            return self.configCloudSigma(configfile, config , password )

        if config.has_section('ProfitBricks'):
            return self.configProfitBricks(configfile, config , password )

        return None

    def configProfitBricks(self, configfile, config, password):
         # generic for other clouds
        (imagedir, image_placement, imagetype) = self.getImageOptions(config)
        volumes = self.createVolumesList(config , configfile, imagedir, imagetype)        
        factory = self.createImageFactory(config , image_placement , imagetype)


        # Azure-specific
        account = config.get('ProfitBricks', 'user')

        region = config.get('ProfitBricks', 'region')

        adjust_override = self.getOverrides(config , configfile)
   
        image = ProfitBricksConfig.ProfitBricksMigrateConfig(volumes , factory, 'x86_64' , imagetype)
        cloud = ProfitBricksConfig.ProfitBricksCloudOptions(account , password, region , imagetype)

        return (image,adjust_override,cloud)


    def configCloudSigma(self, configfile, config, password):
        """gets generic parameters for all clouds"""
        # generic for other clouds
        (imagedir, image_placement, imagetype) = self.getImageOptions(config)
        volumes = self.createVolumesList(config , configfile, imagedir, imagetype)        
        factory = self.createImageFactory(config , image_placement , imagetype)

        # Azure-specific
        user = config.get('CloudSigma', 'user')

        container_name = "cloudscraper"+datetime.date.today().__str__()
        if config.has_option('CloudSigma', 'container'):
           container_name = config.get('CloudSigma', 'container') 
        
        instancetype = "small"
        if config.has_option('CloudSigma', 'instance-type'):
           instancetype = config.get('CloudSigma', 'instance-type')

        region = config.get('CloudSigma', 'region')
        arch = config.get('CloudSigma', 'target-arch')
   
        adjust_override = self.getOverrides(config , configfile)         
   
        image = CloudSigmaConfigs.CloudSigmaMigrateConfig(volumes , factory, arch , imagetype)
        cloud = CloudSigmaConfigs.CloudSigmaCloudOptions( region, user , password, "cloudscraper"+datetime.date.today().__str__())

        return (image,adjust_override,cloud)

    def configAzure(self , configfile , config,  password):
        """gets generic parameters for all clouds"""
        # generic for other clouds
        (imagedir, image_placement, imagetype) = self.getImageOptions(config)
        volumes = self.createVolumesList(config , configfile, imagedir, imagetype)        
        factory = self.createImageFactory(config , image_placement , imagetype)


        # Azure-specific
        account = config.get('Azure', 'storage-account')

        region = config.get('Azure', 'region')

        container_name = "cloudscraper"+datetime.date.today().__str__()
        if config.has_option('Azure', 'container-name'):
           container_name = config.get('Azure', 'container-name') 
        
        instancetype = "small"
        if config.has_option('Azure', 'instance-type'):
           instancetype = config.get('Azure', 'instance-type')

        subscription = ""
        if config.has_option('Azure', 'subscription-id'):
           subscription = config.get('Azure', 'subscription-id')

        certpath = ""
        if config.has_option('Azure', 'certificate-selection'):
           certpath = config.get('Azure', 'certificate-selection')

        affinity = ""
        if config.has_option('Azure', 'affinity-group'):
           affinity = config.get('Azure', 'affinity-group')

        network = ""
        if config.has_option('Azure', 'virtual-network'):
           network = config.get('Azure', 'virtual-network')

        subnet = ""
        if config.has_option('Azure', 'virtual-subnet'):
           subnet = config.get('Azure', 'virtual-subnet')
        

        adjust_override = self.getOverrides(config , configfile)
   
        image = AzureConfigs.AzureMigrateConfig(volumes , factory, 'x86_64' , imagetype)
        cloud = AzureConfigs.AzureCloudOptions(account , password, container_name , region , subscription, certpath, instancetype , affinity=affinity or network , subnet=subnet)

        return (image,adjust_override,cloud)

    #returns the tuple containing the config info (Image,Fixups,Cloud)
    def configAmazon(self , configfile , user = '' , password = '' , region = '', imagepath = ''):

        config = UnicodeConfigParser.UnicodeConfigParser()
        config.readfp(codecs.open(configfile, "r", "utf16"))

        import os
        if os.name == 'nt':
            sys.path.append('./Windows')
            import Windows
            imagesize = Windows.Windows().getSystemInfo().getSystemVolumeInfo().getSize()  
        else:
            sys.path.append('./Linux')
            import Linux
            imagesize = Linux.Linux().getSystemInfo().getSystemVolumeInfo().getSize() 

        #cloud config
        if user == '':
            user = config.get('EC2', 's3key')
        if password == '':
            password = config.get('EC2', 's3secret')
        
        instancetype = config.get('EC2', 'instance-type')

        if region == '':
            region = config.get('EC2', 'region')

        arch = config.get('EC2', 'target-arch')

        imagearch = config.get('Image', 'source-arch')
            
        s3prefix = ""
        if config.has_option('EC2', 's3prefix'):
           s3prefix = config.get('EC2', 's3prefix') 
       
        vpcsubnet = ""
        if config.has_option('EC2', 'vpcsubnet'):
           vpcsubnet = config.get('EC2', 'vpcsubnet') 
       
        
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
        
        (imagedir, image_placement, imagetype) = self.getImageOptions(config)
        volumes = self.createVolumesList(config , configfile, imagedir, imagetype , s3prefix)        
        factory = self.createImageFactory(config , image_placement , imagetype)

        newsize = imagesize
        installservice = None;

        adjust_override = self.getOverrides(config , configfile)

        image = AmazonConfigs.AmazonMigrateConfig(volumes , factory, imagearch , imagetype)
        #TODO: add machine name
        cloud = AmazonConfigs.AmazonCloudOptions(bucket = bucket , user=user , password=password , newsize=newsize , arch=arch , zone= zone , region=region , machinename="" , securityid=security , instancetype=instancetype \
                                                , disktype = imagetype , keyname_prefix = s3prefix , vpc=vpcsubnet)
        

        return (image,adjust_override,cloud)

    #TODO: move the common code to one function
    def configElasticHosts(self , configfile , user = '' , password = '' , region = '', imagepath = ''):
        config = UnicodeConfigParser.UnicodeConfigParser()
        config.readfp(codecs.open(configfile, "r", "utf16"))

        
        if os.name == 'nt':
            sys.path.append('./Windows')
            import Windows
            imagesize = Windows.Windows().getSystemInfo().getSystemVolumeInfo().getSize()  
        else:
            sys.path.append('./Linux')
            import Linux
            imagesize = Linux.Linux().getSystemInfo().getSystemVolumeInfo().getSize() 
        imagetype = 'VHD'      

        #cloud config
        if user == '':
            user = config.get('ElasticHosts', 'user-uuid')
        if password == '':
            password = config.get('ElasticHosts', 'ehsecret')
        
        if region == '':
            region = config.get('ElasticHosts', 'region')

        avoiddisks = ""
        if config.has_option('ElasticHosts', 'avoid-disks'):
           avoiddisks = config.get('ElasticHosts', 'avoid-disks') 
           #making it just space-separated
           avoiddisks = avoiddisks.replace("; ", " ");
           #TODO: make a list so it could be iterated thru in case we need to use for checking something
        

        
        (imagedir, image_placement, imagetype) = self.getImageOptions(config)
        volumes = self.createVolumesList(config , configfile, imagedir ,imagetype)        
        factory = self.createImageFactory(config , image_placement , imagetype)
      
        newsize = imagesize
        
        adjust_override = self.getOverrides(config , configfile)

        image = EHConfigs.EHMigrateConfig(volumes , factory, 'x86_64' , imagetype)
        cloud = EHConfigs.EHCloudOptions( user , password , newsize , 'x86_64' , region, avoiddisks)

        return (image,adjust_override,cloud)

    def getOverrides(self, config , configfile):
        """returns dict() of overrides"""
        #override copy-paste
        adjust_override = dict()
        if config.has_section('Service'):
            service_test = True
            if config.has_option('Service', 'test'):
                service_test = config.getboolean('Service', 'test')
            installpath = "..\\..\\service\\dist"
            if config.has_option('Service', 'installpath'):
                service_test = config.getboolean('Service', 'installpath')
            
            adjust_override = self.getServiceOverrides(config , configfile, installpath , test=service_test)

        if config.has_section('Fixes'):
            # not very nice but should work
            adjust_override.update(config._sections['Fixes'])

        return adjust_override

    def createVolumesList(self , config, configfile, imagedir , imagetype , upload_prefix = ""):
        """creates volume list"""
        volumes = list()
        if config.has_section('Volumes') :
            if config.has_option('Volumes', 'letters'):
                letters = config.get('Volumes', 'letters') 
               
                for letter in letters.split(','):
                    if os.name == 'nt':
                        devicepath = '\\\\.\\'+letter+':'
                        sys.path.append('./Windows')
                        import Windows
                        size = Windows.Windows().getSystemInfo().getVolumeInfo(letter+":").getSize()
                    else:
                        devicepath = "/dev/"+letter
                        sys.path.append('./Linux')
                        import Linux
                        size = Linux.Linux().getSystemInfo().getVolumeInfo(devicepath).getSize()
                    volume = VolumeMigrateIniConfig(config , configfile , letter , devicepath)
                    if volume.getImagePath() == '':
                        volume.setImagePath(imagedir+"\\"+letter+"."+imagetype);
                    if volume.getImageSize() == 0:
                        volume.setImageSize(size)
                    if volume.getUploadPath() == '':
                        volume.setUploadPath(upload_prefix+os.environ['COMPUTERNAME']+"-"+letter)
                    volumes.append( volume )
        return volumes

    def createImageFactory(self , config , image_placement , imagetype):
        """generates factory to create media (virtual disk files) to store image before upload"""
        compression = 2
        if config.has_option('Image', 'compression'):
            compression =  config.getint('Image', 'compression')
        
        # check run on windows flag
        factory = None
        if (imagetype == "VHD" or imagetype == "fixed.VHD") and image_placement == "local":
            if os.name == 'nt':
                import WindowsVhdMediaFactory
                factory = WindowsVhdMediaFactory.WindowsVhdMediaFactory(fixed = (imagetype == "fixed.VHD"))
            else:
                logging.error("!!!ERROR: Linux doesn't support VHD format");
            
        #if imagetype == "raw.gz" and image_placement == "local":
        # factory =  RawGzipMediaFactory.RawGzipMediaFactory(imagepath , imagesize)
        if (imagetype == "raw.tar" or imagetype == "RAW") and image_placement == "local":
            chunk = 4096*1024
            factory = GzipChunkMediaFactory.GzipChunkMediaFactory(chunk , compression)
        if (imagetype == "stm.vmdk" or imagetype == "vmdk" or imagetype == "VMDK") and image_placement == "local":
            factory = StreamVmdkMediaFactory.StreamVmdkMediaFactory(compression) 
        return factory

    def getImageOptions(self , config):
        """gets tuple of image related data (image placement , image types , image path (directory) ) """
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
            logging.debug("Directory " + unicode(imagedir) + " not found, creating it");
            os.mkdir(imagedir)           
        else:
            dirmode = os.stat(imagedir).st_mode
            if stat.S_ISDIR(dirmode) == False:
                #TODO: create wrapper for error messages
                #TODO: test UNC path
                logging.error("!!!ERROR Directory given for image storage is not valid!") 

        image_placement = ""
        if config.has_option('Image', 'image-placement'):
           image_placement = config.get('Image', 'image-placement') 
        else:
           image_placement = "local"

        return (imagedir, image_placement, imagetype)

    def getServiceOverrides(self, config, configfile, installpath , test=False):
        #here the service config is being generated
        #TODO: separate override from ini creation
        #TODO: ini and service should be on Windows drive! Make a separate function for this matter
        servicepath = "..\\..\\service\\"
        service_config = installpath + "\\service-"+str(int(time.mktime(time.localtime())))+".ini"
        if not test:
            defaultpath = servicepath + "default-non-test.ini";
        else:
            defaultpath = servicepath + "default-test.ini";

        srcfile = open(defaultpath , "r")
        data = srcfile.read()
        data = data.replace("<TRANSFER-INI>" , os.path.abspath(configfile))
        srcfile.close()

        dstfile = open(service_config, "w")
        dstfile.write(data)
        dstfile.close()

        service_exe = os.path.abspath(installpath+"\\CloudscraperServiceMain.exe");  
        
        # looks ugly
        
        override = dict()
        override['service-config-path'] = os.path.abspath(service_config)
        override['service-bin-path'] = service_exe
        override['service-install'] = True

        return override