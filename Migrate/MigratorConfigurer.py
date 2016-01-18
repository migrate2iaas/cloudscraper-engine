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
import onAppConfigs
import OpenStack
from OpenStack import OpenStackConfigs

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
import SparseRawMediaFactory
import VhdQcow2MediaFactory
import SparseRawMedia
import WindowsVhdMedia


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
            logging.warn("! Section for drive letter cannot be found") 
            return


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

        logging.debug("Config read:" + repr(config))
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

        if config.has_section('onApp'):
            return self.configOnApp(configfile, config , password )

        if config.has_section('OpenStack'):
            return self.configOpenStack(configfile, config , password )

        logging.error("!!!ERROR: No appropriate config entry found. Config is corrupted or target cloud is not supported by the software version")
        return None

    def configOpenStack(self , configfile, config, password):
        (imagedir, image_placement, imagetype) = self.getImageOptions(config)
        volumes = self.createVolumesList(config , configfile, imagedir, imagetype)        
        factory = self.createImageFactory(config , image_placement , imagetype)
        
        user = config.get('OpenStack', 'user')
        endpoint = config.get('OpenStack', 'endpoint')
        tennant = config.get('OpenStack', 'tennant')
        network = None
        if config.has_option('OpenStack', 'network'):
            network = config.get('OpenStack', 'network')

        flavor = None
        if config.has_option('OpenStack', 'flavor'):
            flavor = config.get('OpenStack', 'flavor')

        ip_pool = None
        if config.has_option('OpenStack', 'ip_pool'):
            ip_pool = config.get('OpenStack', 'ip_pool')

        container = "bare"
        if config.has_option('OpenStack', 'container'):
            network = config.get('OpenStack', 'container')
        #TODO: we may add flavors here

        # Swift specific parameters (if different from the major cloud ones)
        swift_server_url = endpoint
        if config.has_option('OpenStack', 'swift_endpoint'):
            swift_server_url = config.get('OpenStack', 'swift_endpoint')

        swift_tennant_name = tennant
        if config.has_option('OpenStack', 'swift_tennant'):
            swift_tennant_name = config.get('OpenStack', 'swift_tennant')

        swift_username = user
        if config.has_option('OpenStack', 'swift_user'):
            swift_username = config.get('OpenStack', 'swift_user')

        swift_password = user
        if config.has_option('OpenStack', 'swift_password'):
            swift_password = config.get('OpenStack', 'swift_password')

        swift_container = "cloudscraper-"+str(int(time.time()))
        if config.has_option('OpenStack', 'swift_container'):
            swift_container = config.get('OpenStack', 'swift_container')

        swift_compression = 0
        if config.has_option('OpenStack', 'swift_compression'):
            swift_compression = config.get('OpenStack', 'swift_compression')

        use_new_channel = False
        if config.has_option('OpenStack', 'use_new_channel'):
            use_new_channel = config.get('OpenStack', 'use_new_channel')

        ignore_etag = False
        if config.has_option('OpenStack', 'ignore_etag'):
            ignore_etag = config.get('OpenStack', 'ignore_etag')
            
        adjust_override = self.getOverrides(config, configfile)
        manifest_path, increment_depth = self.loadDRconfig(config)
        image = OpenStackConfigs.OpenStackMigrateConfig(volumes, factory, 'x86_64', imagetype)
        cloud = OpenStackConfigs.OpenStackCloudOptions(
            endpoint, user, tennant, password, network, imagetype, container, flavor=flavor, ip_pool_name=ip_pool,
            swift_server_url=swift_server_url, swift_tennant_name=swift_tennant_name, swift_username=swift_username,
            swift_password=swift_password, swift_container=swift_container, compression=swift_compression,
            use_new_channel=use_new_channel, manifest_path=manifest_path, increment_depth=increment_depth,
            ignore_etag=ignore_etag)
        return (image, adjust_override, cloud)

    def configOnApp(self, configfile, config, password):
         # generic for other clouds
        (imagedir, image_placement, imagetype) = self.getImageOptions(config)
        volumes = self.createVolumesList(config , configfile, imagedir, imagetype)        
        factory = self.createImageFactory(config , image_placement , imagetype)

        onapp_login = config.get('onApp', 'user')
        onapp_endpoint = config.get('onApp', 'endpoint')
        onapp_datastore_id = config.get('onApp' , 'datastore')
        onapp_port = 80
        if config.has_option('onApp', 'port'):
            onapp_port = config.get('onApp', 'port')
        
        minipad_ip = None
        if config.has_option('onApp', 'minipad_ip'):
            minipad_ip = config.get('onApp', 'minipad_ip') 

        minipad_template = ""
        if config.has_option('onApp', 'minipad_template'):
            minipad_template = config.get('onApp', 'minipad_template') 

        minipad_vm_id = "" 
        if config.has_option('onApp', 'minipad_vm_id'):
            minipad_vm_id = config.get('onApp', 'minipad_vm_id') 

        vm_build_timeout = 120*60
        if config.has_option('onApp', 'vm_build_timeout'):
            vm_build_timeout = config.get('onApp', 'vm_build_timeout') 

        wintemplate_size = 20
        if config.has_option('onApp', 'wintemplate_size'):
            wintemplate_size = int(config.get('onApp', 'wintemplate_size') )


        onapp_target_account = None

        s3bucket = config.get('onApp', 's3bucket')
        s3user = config.get('onApp', 's3user')
        s3secret = config.get('onApp', 's3secret')
        s3region = config.get('onApp', 's3region')

        adjust_override = self.getOverrides(config , configfile)

        image = onAppConfigs.onAppMigrateConfig(volumes , factory, 'x86_64' , imagetype)
        cloud = onAppConfigs.onAppCloudOptions(s3bucket , s3user , s3secret , s3region , onapp_endpoint , onapp_login , \
            password , onapp_datastore_id, onapp_target_account , onapp_port = onapp_port, preset_ip = minipad_ip, \
            minipad_image_name = minipad_template , minipad_vm_id = minipad_vm_id , vmbuild_timeout_sec = int(vm_build_timeout) , wintemplate_size = wintemplate_size)

        return (image,adjust_override,cloud)


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
        try:
            config = UnicodeConfigParser.UnicodeConfigParser()
            config.readfp(codecs.open(configfile, "r", "utf16"))
        except Exception as e:
            logging.info("Couldn't read an config as unicode file due to " + str(e) + " . Reading as ascii")
            config = ConfigParser.RawConfigParser()
            config.read(configfile)

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

        #custom S3\EC2 compatible clouds
        custom_host = ""
        if config.has_option('EC2', 'host'):
           custom_host = config.get('EC2', 'host') 
        
        custom_port = 80
        if config.has_option('EC2', 'port'):
           custom_port = int(config.get('EC2', 'port'))
       
        custom_suffix = ""
        if config.has_option('EC2', 'suffix'):
           custom_suffix = config.get('EC2', 'suffix') 

        use_ssl = ""
        if config.has_option('EC2', 'ssl'):
           use_ssl = config.get('EC2', 'ssl')
           
        chunksize = 10*1024*1024
        if config.has_option('EC2', 'chunksize'):
           chunksize = int(config.get('EC2', 'chunksize'))

        bucket = ''

        try:
            bucket = config.get('EC2', 'bucket')
            bucket = str(bucket).lower()
        except ConfigParser.NoOptionError as exception:
            logging.info("No bucket name found, generating a new one")

        if bucket == '':
            #TODO: make another time mark: minutes-seconds-machine-name?
            bucket = "cloudscraper-" + str(int(time.mktime(time.localtime())))+"-"+region 
        
        config.set('EC2', 'bucket' , bucket)

        
        security = "default"
        try:
            security = config.get('EC2', 'security-group')
        except ConfigParser.NoOptionError as exception:
            logging.info("No security group was speicified, using default one. Note it couldn't be changed afterwards.")
        
        (imagedir, image_placement, imagetype) = self.getImageOptions(config)
        volumes = self.createVolumesList(config, configfile, imagedir, imagetype, s3prefix)
        factory = self.createImageFactory(config, image_placement, imagetype)

        use_dr = False
        manifest_path = increment_depth = None
        if config.has_section('DR'):
            use_dr = True
            manifest_path, increment_depth = self.loadDRconfig(config)

        newsize = imagesize
        installservice = None

        adjust_override = self.getOverrides(config, configfile)

        image = AmazonConfigs.AmazonMigrateConfig(volumes, factory, imagearch, imagetype)
        #TODO: add machine name
        cloud = AmazonConfigs.AmazonCloudOptions(
            bucket=bucket, user=user, password=password, newsize=newsize, arch=arch, zone=zone, region=region,
            machinename="", securityid=security, instancetype=instancetype, chunksize=chunksize, disktype=imagetype,
            keyname_prefix=s3prefix, vpc=vpcsubnet, custom_host=custom_host, custom_port=custom_port,
            custom_suffix=custom_suffix, use_ssl=use_ssl, manifest_path=manifest_path, increment_depth=increment_depth,
            use_dr=use_dr)
        

        return (image,adjust_override,cloud)

    #TODO: move the common code to one function
    def configElasticHosts(self, configfile, user='', password='', region='', imagepath=''):
        try:
            config = UnicodeConfigParser.UnicodeConfigParser()
            config.readfp(codecs.open(configfile, "r", "utf16"))
        except Exception as e:
            logging.info("Couldn't read an config as unicode file due to " + str(e) + " . Reading as ascii")
            config = ConfigParser.RawConfigParser()
            config.read(configfile)

        
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
            
            #check what volumes to migrate
            letters=""
            letterslist = list()
            if config.has_option('Volumes', 'letters'):
                letters = config.get('Volumes', 'letters')                 
            letterslist = letters.split(',')

            # if system is set , add autolocated system volume by default
            if config.has_option('Volumes', 'system'):
                addsys = config.getboolean('Volumes', 'system') 
                if addsys:
                    sysvol = os.environ['windir'].split(':')[0] #todo: change to cross-platform way
                    if not sysvol in letterslist:
                        letterslist.append(sysvol)

            for letter in letterslist:
                if not letter:
                    continue
                if os.name == 'nt':
                    devicepath = '\\\\.\\'+letter+':'
                    sys.path.append('./Windows')
                    import Windows
                    size = Windows.Windows().getSystemInfo().getVolumeInfo(letter+":").getSize()
                else:
                    if not "/dev/" in letter: 
                        devicepath = "/dev/"+letter
                    else:
                        devicepath = letter
                    sys.path.append('./Linux')
                    import Linux
                    size = Linux.Linux().getSystemInfo().getVolumeInfo(devicepath).getSize()
                volume = VolumeMigrateIniConfig(config , configfile , letter , devicepath)
                if volume.getImagePath() == '':
                    volume.setImagePath(imagedir+"/"+letter+"."+imagetype);
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
        if (imagetype == "raw.tar" or imagetype.lower() == "raw") and image_placement == "local":
            chunk = 4096*1024
            factory = GzipChunkMediaFactory.GzipChunkMediaFactory(chunk , compression)
        if (imagetype == "stm.vmdk" or imagetype.lower() == "vmdk") and image_placement == "local":
            factory = StreamVmdkMediaFactory.StreamVmdkMediaFactory(compression) 
        if (str(imagetype).lower() == "sparsed" or imagetype.lower() == "sparsed.raw"):
            factory = SparseRawMediaFactory.SparseRawMediaFactory()

        #Here we can do some additional conversation using qemu utilities
        if (config.has_option('Qemu', 'path') and config.has_option('Qemu', 'dest_imagetype')):
            qemu_path = config.get('Qemu', 'path')
            dest_imagetype = config.get('Qemu', 'dest_imagetype')
            qemu_convert_params = ""
            if config.has_option('Qemu', 'qemu_convert_params'):
                qemu_convert_params = int(config.get('Qemu', 'qemu_convert_params'))
            factory = VhdQcow2MediaFactory.VhdQcow2MediaFactory(factory , qemu_path , dest_imagetype , qemu_convert_params = qemu_convert_params)

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

    def loadDRconfig(self, config):
        manifest_path = 'C:\\backup-manifest'
        if config.has_option('DR', 'manifest_path'):
            manifest_path = config.get('DR', 'manifest_path')

        increment_depth = 1
        if config.has_option('DR', 'increment_depth'):
            increment_depth = config.get('DR', 'increment_depth')

        return manifest_path, increment_depth

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
