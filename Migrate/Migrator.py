# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import AdjustedBackupSource
import logging
import traceback
import sys
import os
import stat
import DataExtent
import datetime
import time
import random
import VmInstance
import VmVolume

import RawGzipMedia
import SimpleDiskParser
import SimpleDataTransferProto

import MigrateConfig
import CloudConfig
import SystemAdjustOptions
import GzipChunkMedia
import SimpleTransferTarget
import ProxyTransferTarget
import VmInstance

from MigrateExceptions import FileException, AccessDeniedException

class Migrator(object):
    """Here I came to the trap of all products: make kinda place with function DO-EVERYTHING-I-WANT reside"""


    def __init__(self , cloud_options , migrate_options, sys_adjust_overrides , skip_imaging=False, resume_upload=False, skip_upload=False ,\
        self_checks=False , limits = None , insert_vitio = False , insert_xen = False, backup_mode = False):
        """
        Inits the Migrator mega-class. 

        Args:
            cloud_options: CloudConfig.CloudConfig - options specifiying cloud parameters. These options are passed to cloud instance generators and upload channels
            migrate_options: MigrateConfig.MigrateConfig - options specifiying how the image should be migrated. These options alters the Migrator behaviour
            sys_adjust_overrides: dict - dict-like class overriding default options specifying how the resulting system should be adjusted
            skip_imaging: bool - flag to skip imaging activities. Needed primarily for resume-upload scenarios
            resume_upload: bool - flag to resume upload instead of starting new one. Needed primarily for resume-upload scenarios
            skip_upload: bool - flag to skip upload at all. Needed primarily in case when the upload is already done but cloud server is not created yet
            self_checks: bool - some self-checks on images\registry during the Migrator work (doesn't work for now!)
            limits: ? (currently long) - the limitation of data to be transfered
            insert_vitio : bool - inserts virtio drivers to the running system. Note, this option can be overriden with migrate_options.insertVirtIo()
            insert_xen : bool - inserts xen drivers to the running system. Note, this option can be overriden with migrate_options.insertXen()
            backup_mode : bool - doesn't create a VM inside the cloud. Just stores image there
        """
        self.__adjustedSystemBackupSource = None
        self.__systemBackupSource = None
        self.__systemTransferTarget = None
        self.__systemAdjustOptions = None
        # the channel is underlying connection to transfer the data for system target
        self.__systemTransferChannel = None
        self.__systemMedia = None

        self.__limitUpload = limits

        self.__adjustOption = None
        self.__migrateOptions = migrate_options
        self.__cloudOptions = cloud_options
        
        self.__dataBackupSourceList = dict() # the key is data volume device path
        self.__dataTransferTargetList = dict()
        self.__dataChannelList = dict()
        self.__dataMediaList = dict()
        
        self.__runOnWindows = False
        
        self.__selfChecks = self_checks
        self.__skipImaging = skip_imaging
        self.__skipUpload = skip_upload
        self.__resumeUpload = resume_upload
        self.__backupMode = backup_mode
       
        self.__resultingInstance = None

        self.__cloudName = self.__cloudOptions.getTargetCloud()

        #extra megabyte of additional size to store mbr, etc on the image
        self.__additionalMediaSize = 0x800*0x200
        self.__os = None
        
        self.__fileBackup = False
        self.__insertVirtio = insert_vitio or migrate_options.insertVirtIo()
        self.__insertXen = insert_xen or migrate_options.insertXen()

        self.__error = False


        #TODO: pass this parm somehow. Thru migrate\adjust overrides?
        self.__linuxGC = True
        
        #TODO: analyze both host and source systems
        if self.__migrateOptions.getHostOs() == "Windows":
            import Windows
            self.__windows = Windows.Windows(self.__insertVirtio , self.__insertXen)
            self.__runOnWindows = True
            self.__winSystemAdjustOptions = self.__windows.createSystemAdjustOptions(sys_adjust_overrides)
            self.__systemAdjustOptions = self.__winSystemAdjustOptions
            self.__os = self.__windows
            #self.__winSystemAdjustOptions
        else:
            if self.__linuxGC:
                import Linux_GC
                self.__linux = Linux_GC.Linux()
            else:
                import Linux
                self.__linux = Linux.Linux()
            self.__systemAdjustOptions = self.__linux.createSystemAdjustOptions()
            self.__os = self.__linux


    # runs full scenario from the start to the upload
    def runFullScenario(self):
        """runs full scenario doing all the stuff needed to migrate the server and create a cloud server. returns instance object"""
        try:
            logging.info("Generating migration process step handlers...")
            # 0) check the input action config: what to do
            self.generateStepHandlers()
        
            if self.__migrateOptions.getSystemVolumeConfig():
                logging.info("Checking system compatibility...")
                # 1) test the system on compatibility
                if self.checkSystemCompatibility() == False:
                    logging.info(">>>>>>>>>>>> Compatipility check failed")
                    return None

                logging.info("Checking the input config params")
                # 2) check the input parms
                if self.checkInputParams() == False:
                    logging.info(">>>>>>>>>>>> Parameter check failed")
                    return None

                logging.info("Creating the transfer target")
                #NOTE: disk size should be min 1 mb larger (offset from mbr)
                # 3) create virtual image (transfer target)
                if self.createSystemTransferTarget() == False:
                    logging.info(">>>>>>>>>>>> Transfer target creation failed")
                    return None
        
                # 4) gets system backup source
                logging.info("Initializing system copy source")
                if self.createSystemBackupSource() == False:
                    logging.info(">>>>>>>>>>>> Couldn't adjust the copy source")
                    return None

                # 5) adjusts the system backup source
                logging.info("Adjusting the system copy parms")
                if self.adjustSystemBackupSource() == False:
                    logging.info(">>>>>>>>>>>> Couldn't adjust the copy parms")
                    return None

                # 6) adjust transfer target to fit our needs
                logging.info("Adjusting the copy target")
                if self.adjustSystemBackupTarget() == False:
                    logging.info(">>>>>>>>>>>> Couldn't adjust the copy target")
                    return None

                # 7) starts the data transfer
                logging.info("System copy started")
                if self.startSystemBackup() == False:
                    logging.info(">>>>>>>>>>>> System copy failed")
                    return None
            else:
                logging.info(">>>>>>>>>>>>>>>>>>>>>>>>>> No system volumes specified for transfer") 

            # 8) TODO: instance creation should be distinct start()

            if self.__migrateOptions.getDataVolumes().__len__():
                logging.info(">>>>>>>>>>>>>>>>>>>>>>>>>> Migrating %d data volumes" , self.__migrateOptions.getDataVolumes().__len__()) 

                # 9) create data images\upload targets. that could be done in several ways
                logging.info("Initializing data transfer target")
                if self.createDataTransferTargets() == False:
                    logging.info("Transfer target creation failed")
                    return None
        
                #TODO: create source adjust too

                # 10) gets system backup source
                logging.info("Initializing the system backup")
                if self.createDataBackupSources() == False:
                    logging.info("Couldn't adjust the copy source")
                    return None

                # 11) adjusts data backup source
                logging.info("Adjusting the data copy parms")
                if self.adjustDataBackupSource() == False:
                    logging.info(">>>>>>>>>>>> Couldn't adjust the copy parms")
                    return None

                # 11) adjust transfer target to fit our needs
                logging.info("Adjusting the copy target")
                if self.adjustDataBackupTarget() == False:
                    logging.info("Couldn't adjust the copy target")
                    return None

                # 12) starts the data transfer
                logging.info("System copy started")
                if self.startDataBackup() == False:
                    logging.info("System copy failed")
                    return None

            # 13) TODO: attach volumes here!
            
            if self.__resultingInstance and isinstance(self.__resultingInstance, VmInstance.VmInstance):
                logging.info("Finalizing the instance making it ready for the user")
                self.__resultingInstance.finalize()
        

        except Exception as ex:
            traceback.print_exception(sys.exc_info()[0] , sys.exc_info()[1] , sys.exc_info()[2]) 
            logging.error("!!!ERROR: Unexpected error occured " + str(ex))
            logging.error(traceback.format_exc())
            self.__error = True
            return None
            #TODO: set error state
            # do cleanups? Should write something to the console
            
        finally:
            for channel in self.__dataChannelList.values():
                channel.close()
            if self.__systemTransferChannel:
                self.__systemTransferChannel.close()

            if self.__systemTransferTarget:
                self.__systemTransferTarget.close()
            for target in self.__dataTransferTargetList.values():
                target.close()
            

        # TODO: catch and free resources here! registry, files, vhds, snapshots.
        # use context manager for that
        return self.__resultingInstance

    def getError(self):
        """returns true if any errors occur during the migration"""
        return self.__error
            

    def checkSystemCompatibility(self):
        sys_info = self.__os.getSystemInfo()
        logging.info("System version: " + sys_info.getSystemVersionString() + " arch:" + hex(sys_info.getSystemArcheticture())) 
        if self.__runOnWindows:
            if (sys_info.getSystemArcheticture() == sys_info.Archx8664 or sys_info.getSystemArcheticture() == sys_info.Archi386) and sys_info.getKernelVersion() >= sys_info.Win2003:
                return True
            logging.error("!!!ERROR: The configuration is not supported " + sys_info.getSystemVersionString() + " arch:" + hex(sys_info.getSystemArcheticture())) 
            logging.info("Windows 2003 , 2008R2 and 2012 Server are supported for now") 
        else: 
            logging.warning("!Warning: Linux is in experimental support mode")
            return True
        return False

    #reads the config and generates appropriate handlers for the each step
    def generateStepHandlers(self):
        return True

    def generateSystemDataBackupSource(self):
        return self.__os.getSystemDataBackupSource()

    def generateDataBackupSource(self , volume):
        return self.__os.getDataBackupSource(volume)

    def createSystemBackupSource(self):
        """creates the system backup source, inits the system disk snapshot"""
        if self.__skipImaging:
            return True
        if self.__runOnWindows:
            import WindowsBackupSource
            import WindowsBackupAdjust
            self.__systemBackupSource = WindowsBackupSource.WindowsBackupSource()
            self.__adjustOption = WindowsBackupAdjust.WindowsBackupAdjust(
                self.__winSystemAdjustOptions, self.__windows.getVersion())
        else:
            if self.__linuxGC:
                import Linux_GC
                from Linux_GC.LinuxBackupSource import LinuxBackupSource
                from Linux_GC.LinuxBackupAdjust import LinuxBackupAdjust
                self.__systemBackupSource = LinuxBackupSource("/")
                self.__adjustOption = LinuxBackupAdjust()
                self.__fileBackup = True
            else:
                import Linux
                from Linux.LinuxBackupSource import LinuxBackupSource
                from Linux.LinuxBackupAdjust import LinuxBackupAdjust
                self.__systemBackupSource = LinuxBackupSource()
                self.__adjustOption = LinuxBackupAdjust()

        self.__systemBackupSource.setBackupDataSource(self.generateSystemDataBackupSource())
        
        return True

    def adjustSystemBackupSource(self):
        """configures all adjusts needed for the system to be backed-up to transfer target"""
        if self.__skipImaging:
            return True

        self.__adjustedSystemBackupSource = AdjustedBackupSource.AdjustedBackupSource()
        self.__adjustedSystemBackupSource.setBackupSource(self.__systemBackupSource)

        self.__adjustOption.configureBackupAdjust(self.__systemBackupSource, self.__migrateOptions.getDataVolumes())
        # remove excluded files and dirs
        excludeddirs = self.__migrateOptions.getSystemVolumeConfig().getExcludedDirs()
        for excluded in excludeddirs:
            logging.info("Removing the file contents from directory " + str(excluded))
            try:
                fileenum = self.__systemBackupSource.getFileEnum(excluded)
                for file in fileenum:
                    logging.debug("Contents of file " + str(file) + " is set to removal")
                    self.__adjustOption.removeFile(str(file))
            except AccessDeniedException as e:
                logging.warning("{0}".format(e))
            except FileException:
                raise

        # for every file in a mask do the following. the excludes shopuld be flexible in vdi migration case
        #self.__systemBackupSource.getFileEnum("\\" , mask)
        self.__adjustedSystemBackupSource.setAdjustOption(self.__adjustOption)

        return True

    # it's kinda ImageFactory
    def createImageMedia(self, imagepath, imagesize):
        """creats image media using factory provided via migrate options"""
        factory = self.__migrateOptions.getImageFactory()
        media = factory.createMedia(imagepath , imagesize)
        if media:
            media.open()
        else:
            logging.error("Cannot open or create intermediate media to save the machine state")
        logging.info("Created new media " + repr(media))
        return media

    def createTransferTarget(self , media , size , adjustoptions, mbr, newtarget = True):
        """
        Internal call to make transfer target (disk volume abstraction) based on media and adjust options
        
        Args:
            media :ImageMedia - the media transfer target is based on
            size :long - the size of transfer target (volume)
            adjustoptions :SystemAdjustOptions - note really WindowsAdjustOptions is used. getNewMbrId() is declared there only
            mbr: id of disk in mbr partition table
            newtarget: bool - if the target should be new one or old one could be used (not used, needed to signal the existing container should be re-created)
        """
        if newtarget:

            if self.__runOnWindows == False and self.__linuxGC == True:
                #then create a linux bundle targer
                import Linux_GC
                if ( isinstance(self.__os , Linux_GC.Linux) ):
                    return self.__os.createBundleTransferTarget(media , size , media.allowDirectFileAccess()); # we use old style gce if image can be directly accessed
                else:
                    raise AssertionError("Linux set to Linux GC but __os is not of Linux_GC.Linux type")

            if self.__runOnWindows == False and adjustoptions.getNewSysPartStart() == 0:
                logging.info("Initializing a transfer task of the whole disk")
                return ProxyTransferTarget.ProxyTransferTarget(SimpleDataTransferProto.SimpleDataTransferProto(media))

            parser = SimpleDiskParser.SimpleDiskParser(SimpleDataTransferProto.SimpleDataTransferProto(media) , mbr_id = mbr , default_offset = self.__additionalMediaSize , windows = self.__runOnWindows)
            
            part_type = SimpleDiskParser.SimpleDiskParser.PART_TYPE_NTFS
            fix_nt_boot = True
            if self.__runOnWindows == False:
                part_type = SimpleDiskParser.SimpleDiskParser.PART_TYPE_EXT
                fix_nt_boot = False
            return parser.createTransferTarget(size , fix_nt_boot, part_type )
        return SimpleTransferTarget.SimpleTransferTarget( self.__additionalMediaSize , SimpleDataTransferProto.SimpleDataTransferProto(media) )
        

    def createSystemTransferTarget(self):
        """Creates transfer targets: disk images where the data should be transfered"""
        if self.__skipImaging == False or self.__skipUpload == False:
            self.__systemMedia = self.createImageMedia(self.__migrateOptions.getSystemImagePath() , self.__migrateOptions.getSystemImageSize() + self.__additionalMediaSize)          
            if self.__systemMedia == None:
                logging.error("!!!ERROR: Cannot create/open intermediate image (media) for an operation")
                return
            
            #NOTE: the media should be created nevertheless of the imaging done
            #so , calls are
        if self.__skipImaging == False:
            self.__systemTransferTarget = self.createTransferTarget(self.__systemMedia, self.__migrateOptions.getSystemImageSize(),
                self.__systemAdjustOptions, self.__systemAdjustOptions.getNewMbrId())
        
        sys_vol_info = self.__migrateOptions.getSystemVolumeConfig()

        if self.__skipImaging == False or self.__skipUpload == False:
                data_size = self.__systemMedia.getMaxSize()
                image_size = self.__systemMedia.getImageSize()
        else:
                data_size = image_size = 0
           
        description = os.environ['COMPUTERNAME']+"-"+"system"+"-"+str(datetime.date.today())
        self.__systemTransferChannel = self.__cloudOptions.generateUploadChannel(data_size , self.__cloudOptions.getServerName() or description, \
                sys_vol_info.getUploadPath(), self.__resumeUpload , image_size )
        
        if self.__skipUpload == False:
            self.__systemTransferChannel.initStorage()        
            # update the upload path in config in case it was changed or created by the channel
            uploadpath = self.__systemTransferChannel.getUploadPath()
            logging.debug("The upload channel path is: " + uploadpath)
            sys_vol_info.setUploadPath(uploadpath)
        
        if self.__skipUpload == True and not sys_vol_info.getUploadId():
            uploadpath = self.__systemTransferChannel.getUploadPath()
            upload_ids = self.__systemTransferChannel.findUploadId(".*?"+sys_vol_info.getUploadPath())
            if not upload_ids:
                logging.error("!!!ERROR: Cannot find any matching manifest.xml file")
                return False
            # we pick latest entry due to date
            upload_id = sorted(upload_ids)[-1]
            logging.info(">>>>>> Utilizing "+ upload_id + " restoration point")
            sys_vol_info.setUploadId(upload_id)
            
        return True
        
    def adjustSystemBackupTarget(self):
        # dunno really what could we do here
        return True

    def startSystemBackup(self):

        if self.__skipImaging:
            logging.info("\n>>>>>>>>>>>>>>>>> Skipping the system imaging\n") 
        else:
            logging.info("\n>>>>>>>>>>>>>>>>> Started the system imaging\n") 
            #HERE we create an alternative backup mechanism: that is file backup
            if self.__fileBackup:
                logging.info("\n>>>>>>>>>>>>>>>> Performing file level transfer");
                files = self.__adjustedSystemBackupSource.getFileEnum(root="/")
                for file in files:
                    self.__systemTransferTarget.transferFile(file)
            else:
                logging.info("\n>>>>>>>>>>>>>>>> Performing block level transfer");
        
                #get data
                extents = self.__adjustedSystemBackupSource.getFilesBlockRange()
            
                #TODO: create kinda callbacks for transfers to monitor them
                self.__systemTransferTarget.transferRawData(extents)
                # free VSS snapshot if Windows
                if self.__runOnWindows:
                    self.__windows.freeDataBackupSource(self.__systemBackupSource.getBackupDataSource())

            self.__systemTransferTarget.close()

            
            
        
        # we save the config to reflect the image generated is ready. 
        #TODO: add the creation time here? or something alike? snapshot time too?
        self.__migrateOptions.getSystemVolumeConfig().saveConfig()
        
        imagesize = 0
        disksize = 0
        if self.__skipUpload:
            logging.info("\n>>>>>>>>>>>>>>>>> Skipping the system image upload\n") 
        else:
            logging.info("\n>>>>>>>>>>>>>>>>> Upload of the system image started\n") 
            channel = self.__systemTransferChannel
            media = self.__systemMedia
            imagesize = media.getImageSize()
            disksize = media.getMaxSize()

            imageid = self.uploadImage(media,channel)

            channel.close()
            if imageid:
                self.__migrateOptions.getSystemVolumeConfig().setUploadId(imageid)
                self.__migrateOptions.getSystemVolumeConfig().saveConfig()
            else:
                logging.error("!!!Error: Upload error. Please make a reupload via resume upload")
                return False
  
        
        if self.__backupMode == True:
            #skip imaging
            logging.info(">>>>>>>>>>>>> The image has been succesfully uploaded to: " + self.__migrateOptions.getSystemVolumeConfig().getUploadId())
            logging.info(">>>>>>>>>>>>> Backup mode is enabled: server instance is not created. Enable redeploy mode to DR from the uploaded image.")
            return True

        #creating instance from the uploaded image
        # imagesize here is the size of image file. but getImageSize() is the size of initial volume
        if self.generateInstance(self.__migrateOptions.getSystemVolumeConfig().getUploadId() , imagesize , disksize):
            logging.info(">>>>>>>>>>>>> System volume transfer completed successfully");
            return True
        else:
            self.__error = True
            return False

    def uploadImage(self , media, channel):
        imagesize = media.getImageSize()
        datasize = channel.getTransferChunkSize()
        logging.debug("Upload image of size " + str(imagesize) + " (full source disk data size is " + str(media.getMaxSize()) + " )") 
        dataplace = 0
        datasent = 0

        logging.info(">>>>>>>>>> Data size to upload: " + str(imagesize/1024/1024) + " MB")

        timestart = datetime.datetime.now()
        while dataplace < imagesize:
            percentcomplete = int(float(dataplace) / imagesize * 100);

            logmsg = ""
            if (datasent % 10 == 0):
                logmsg = " Progress:  " + str(percentcomplete) + "% - "
                logmsg = logmsg + str(int(channel.getOverallDataTransfered()/1024/1024)) + " MB uploaded. " ;
                if channel.getOverallDataSkipped():
                    logmsg = logmsg + str(int(channel.getOverallDataSkipped()/1024/1024)) + " MB was already in the cloud. "

                if self.__limitUpload and channel.getOverallDataTransfered() > self.__limitUpload:
                    logging.error("!!!ERROR: Upload limit reached. Please upgrade contact sales@migrate2iaas.com to obtain the license.")
                    return None

                #NOTE: this is a very rough estimate
                timenow = datetime.datetime.now()
                timedelta = datetime.timedelta()
                timedelta = timenow - timestart
                if dataplace and channel.getOverallDataTransfered():
                    totalsec = timedelta.seconds + long(timedelta.days) * 24*60*60
                    if totalsec:
                        secondsleft = int(float(imagesize - dataplace)/float(dataplace/totalsec))
                        days = secondsleft / (3600*24)
                        hours = secondsleft % (3600*24) / 3600
                        minutes = (secondsleft % (3600*24) % 3600) / 60
                        if days:
                            logmsg = logmsg + str(days) + " days "
                        if hours:
                            logmsg = logmsg + str(hours) + " hrs "
                        if minutes:
                            logmsg = logmsg + str(minutes) + " mins "
                        logmsg = logmsg + "left." 

                logging.info( "% "+ logmsg )

            if dataplace + datasize > imagesize:
                datasize = imagesize - dataplace
            data = media.defferedReadImageData(dataplace, datasize) 
            
            dataext = DataExtent.CachedDataExtent(dataplace , datasize)
            dataplace = dataplace + datasize
            dataext.setData(data)
            if channel.uploadData(dataext) == False:
                return None
            datasent = datasent + 1
            
        channel.waitTillUploadComplete()

 

        logmsg = "% The image data has been fully processed. "
        #if channel.getOverallDataSkipped():
        #    logmsg = logmsg + str(int(channel.getOverallDataSkipped()/1024/1024)) + " MB are already in the cloud. "
        logging.info( logmsg + str(int(channel.getOverallDataTransfered()/1024/1024)) + " MB transfered."  )

        logging.info("\n>>>>>>>>>>>>>>>>> Preparing the image uploaded for cloud use")
        imageid = channel.confirm()
        return imageid

    def checkInputParams(self):
        return True

    def generateInstance(self , imageid,  imagesize , volumesize):
        generator = self.__cloudOptions.generateInstanceFactory()
        instancename = self.__migrateOptions.getSystemVolumeConfig().generateMigrationId()

        if not generator:
            logging.info(">>>>>>>>>>>>>>>>> The system image is uploaded. ");
            logging.info("You could start your server via " + self.__cloudName + " management console");
            logging.info(">>>>>>>>>>>>>>>>> Data was uploaded to " + str(imageid))
            self.__resultingInstance = imageid
            return True

        if self.__cloudName == "EC2":
            logging.info("Creating EC2 VM from the image stored at " + str(imageid))
            import EC2InstanceGenerator
            import EC2Instance
            awskey = self.__cloudOptions.getCloudUser()
            awssecret = self.__cloudOptions.getCloudPass()
            #TODO: refactor this stuff
            self.__resultingInstance = generator.makeInstanceFromImage(imageid, self.__cloudOptions , instancename , awskey, awssecret , self.__migrateOptions.getSystemImagePath() , imagesize , volumesize , self.__migrateOptions.getImageType())
        else:
            self.__resultingInstance = generator.makeInstanceFromImage(imageid , self.__cloudOptions , instancename)

        return self.__resultingInstance != None

    #TODO: make parameters optional, refactor
    # if they are not specified - load imageid
    def generateVolume(self , imageid , localimagepath , imagesize , volumesize , description = ""):
        
        generator = None
        volname = ""
        vol = None
        if description:
           volname = description
        else:
           volname = os.environ['COMPUTERNAME']+"_data_volume"
        #TODO: make volume generator separate class
        if self.__cloudName == "EC2":
            import EC2VolumeGenerator
            import EC2Instance
            import EC2Volume
            awskey = self.__cloudOptions.getCloudUser()
            awssecret = self.__cloudOptions.getCloudPass()
            if self.__cloudOptions.generateInstanceFactory():
                generator = EC2VolumeGenerator.EC2VolumeGenerator(self.__cloudOptions.getRegion())
                vol = generator.makeVolumeFromImage(imageid, self.__cloudOptions , awskey, awssecret , localimagepath , imagesize , volumesize , self.__migrateOptions.getImageType())
            else:
                generator = None
        elif self.__cloudName == "Azure":
            import AzureConfigs
            import AzureInstanceGenerator
            generator = self.__cloudOptions.generateInstanceFactory()
        else:
            generator = self.__cloudOptions.generateInstanceFactory()
        
        if generator and not vol:
            vol = generator.makeVolumeFromImage(imageid , self.__cloudOptions , volname)

        if not generator:
            logging.info(">>>>>>>>>>>>>>>>> The data volume image is uploaded ")
            logging.info(">>>>>>>>>>>>>>>>> You could add it to your server via " + self.__cloudName + " management console");
            logging.info(">>>>>>>>>>>>>>>>> Data was uploaded to " + str(imageid))
            return None

        if vol:
            logging.info(">>>>>>>>>>>>>>>>> The data volume " + str(volname) + " created")
            return True

        return None


    def createDataTransferTargets(self):
        for volinfo in self.__migrateOptions.getDataVolumes():

            # INIT IMAGES:
            if self.__skipImaging == False or self.__skipUpload == False:
                media = self.createImageMedia(volinfo.getImagePath() , volinfo.getImageSize() + self.__additionalMediaSize)          
                if media == None:
                    logging.error("!!!ERROR: Cannot create/open intermediate image (media) for an operation")
                    return False
                self.__dataMediaList[volinfo.getVolumePath()]=media
            
            if self.__runOnWindows:
                 #TODO: need kinda redisign the stuff related to system adjusts!
                  if self.__skipImaging == False:
                       self.__dataTransferTargetList[volinfo.getVolumePath()] = self.createTransferTarget(
                            media, volinfo.getImageSize(), self.__winSystemAdjustOptions, volinfo.getMbrId())
            description = os.environ['COMPUTERNAME']+"_"+"data"+"_"+str(datetime.date.today())

            # if we are running from non-local system, data is and image size is unknown
            if self.__skipImaging == False or self.__skipUpload == False:
                data_size = self.__dataMediaList[volinfo.getVolumePath()].getMaxSize()
                image_size = self.__dataMediaList[volinfo.getVolumePath()].getImageSize()
            else:
                data_size = image_size = 0
            
            # INIT CLOUD CONNECTION
            self.__dataChannelList[volinfo.getVolumePath()] = self.__cloudOptions.generateUploadChannel(
                    data_size,
                    self.__cloudOptions.getServerName() or description,
                    volinfo.getUploadPath(),
                    self.__resumeUpload,
                    image_size)
                
            # INIT STORAGE FOR THE UPLOAD
            if self.__skipUpload == False:
                self.__dataChannelList[volinfo.getVolumePath()].initStorage()
                # update the upload path in config in case it was changed or created by the channel
                uploadpath = self.__dataChannelList[volinfo.getVolumePath()].getUploadPath()
                logging.debug("The upload channel path is: " + uploadpath)
                volinfo.setUploadPath(uploadpath)
                
            # FIND OLD UPLOAD RESULT IF POSSIBLE
            if self.__skipUpload == True and not volinfo.getUploadId():
                # tested on aws only
                upload_ids = self.__dataChannelList[volinfo.getVolumePath()].findUploadId(".*?"+volinfo.getUploadPath())
                if not upload_ids:
                    logging.error("!!!ERROR: Cannot find any matching manifest.xml file")
                    return False
                # we pick latest entry due to date
                upload_id = sorted(upload_ids)[-1]
                logging.info(">>>>>> Utilizing "+ upload_id + " restoration point")
                volinfo.setUploadId(upload_id)

        return True
        
    def createDataBackupSources(self):
        if self.__skipImaging:
            return True

        for volinfo in self.__migrateOptions.getDataVolumes():
            asjustedsource = AdjustedBackupSource.AdjustedBackupSource()

            if self.__runOnWindows:
                import WindowsBackupSource
                import WindowsBackupAdjust

                backupsource = WindowsBackupSource.WindowsBackupSource()
                backupsource.setBackupDataSource(self.generateDataBackupSource(volinfo.getVolumePath()))

                # Adding adjusted source options
                asjustedsource.setBackupSource(backupsource)
                asjustedsource.setAdjustOption(WindowsBackupAdjust.WindowsBackupAdjust(
                    self.__systemAdjustOptions,
                    self.__windows.getVersion()))

            else:
                if self.__linuxGC:
                    import Linux_GC
                    from Linux_GC.LinuxBackupSource import LinuxBackupSource
                    from Linux_GC.LinuxBackupAdjust import LinuxBackupAdjust

                    asjustedsource.setBackupSource(LinuxBackupSource())
                    asjustedsource.setAdjustOption(LinuxBackupAdjust())
                    self.__fileBackup = True
                else:
                    import Linux
                    from Linux.LinuxBackupSource import LinuxBackupSource
                    from Linux.LinuxBackupAdjust import LinuxBackupAdjust

                    asjustedsource.setBackupSource(LinuxBackupSource())
                    asjustedsource.setAdjustOption(LinuxBackupAdjust())

            self.__dataBackupSourceList[volinfo.getVolumePath()] = asjustedsource

        return True

    def adjustDataBackupSource(self):
        """configures all adjusts needed for the system to be backed-up to transfer target"""
        if self.__skipImaging:
            return True

        # remove excluded files and dirs
        for volinfo in self.__migrateOptions.getDataVolumes():
            for excluded in volinfo.getExcludedDirs():
                vol_path = str(volinfo.getVolumePath())
                # if exclude dir starts with volume name
                if not str(excluded).startswith("\\"):
                    # compare volume name
                    if vol_path.endswith(str(excluded)[:2]):
                        excluded = str(excluded)[2:]
                    else:
                        continue
                logging.info("Removing the file contents from directory " + str(excluded))
                try:
                    fileenum = self.__dataBackupSourceList[vol_path].getFileEnum(excluded)
                    for file in fileenum:
                        logging.debug("Contents of file " + str(file) + " is set to removal")
                        self.__dataBackupSourceList[vol_path].getAdjustOption().removeFile(str(file))
                except AccessDeniedException as e:
                    # Just logging access denied exception
                    logging.warning("{0}".format(e))
                except FileException:
                    raise

        return True
        

    def adjustDataBackupTarget(self):
        return True

    def startDataBackup(self):
        if self.__skipImaging:
            logging.info("\n>>>>>>>>>>>>>>>>> Skipping the data volume imaging\n") 
        else:
            #RODO: make kinda generic function. I'm pissed with all this copy-paste bugs
            #TODO: log and profile
            logging.info("\n>>>>>>>>>>>>>>>>> Started the data volume imaging\n") 
            for volinfo in self.__migrateOptions.getDataVolumes():
                #get data
                extents = self.__dataBackupSourceList[volinfo.getVolumePath()].getFilesBlockRange()
        
                #TODO: create kinda callbacks for transfers to monitor them
                #write,
                self.__dataTransferTargetList[volinfo.getVolumePath()].transferRawData(extents)

                self.__dataTransferTargetList[volinfo.getVolumePath()].close()
                
                #TODO: move somewhere else
                if self.__runOnWindows:
                    self.__windows.freeDataBackupSource(
                        self.__dataBackupSourceList[volinfo.getVolumePath()].getBackupSource().getBackupDataSource())

                # we save the config to reflect the image generated is ready. 
                #TODO: add the creation time here? or something alike? snapshot time too?
                volinfo.saveConfig()
        
        for volinfo in self.__migrateOptions.getDataVolumes():

            mediaimagesize = 0
            disksize = 0
            if self.__skipUpload:
                logging.info("\n>>>>>>>>>>>>>>>>> Skipping the data image upload\n") 
            else:
                logging.info("\n>>>>>>>>>>>>>>>>> Started the data image upload\n") 

                channel = self.__dataChannelList[volinfo.getVolumePath()]
                media = self.__dataMediaList[volinfo.getVolumePath()]
                imageid = self.uploadImage(media,channel)
                channel.close()
                mediaimagesize = media.getImageSize()
                disksize = media.getMaxSize()
                
                if imageid:
                    volinfo.setUploadId(imageid)
                    volinfo.saveConfig()
                else:
                    logging.error("!!!Error: Upload error. Please make a reupload via resume upload")
                    return False
              
            logging.info("Creating volume from the image stored at " + str(volinfo.getUploadId()))
            
            # image size is really size of data, imagesize here is size of image file
            # dammit, needs clarifications
            volume = self.generateVolume(volinfo.getUploadId() , volinfo.getImagePath() , mediaimagesize , disksize , volinfo.generateMigrationId() )
            if self.__resultingInstance and volume and isinstance(volume,VmVolume.VmVolume):
                logging.info(">>> Attaching volume " + str(volume) + " to instance " + str(self.__resultingInstance))
                try:
                    volume.attach(str(self.__resultingInstance))
                except Exception as e:
                    logging.warning("!Cannot attach volume " + str(volume) + " to the instance. Please attach the volume manually via cloud management console")
                    logging.warning(traceback.format_exc())

       
        return True
