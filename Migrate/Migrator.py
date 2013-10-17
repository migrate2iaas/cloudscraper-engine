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

import RawGzipMedia
import SimpleDiskParser
import SimpleDataTransferProto

import MigrateConfig
import CloudConfig
import SystemAdjustOptions
import GzipChunkMedia
import SimpleTransferTarget

class Migrator(object):
    """Here I came to the trap of all products: make kinda place with function DO-EVERYTHING-I-WANT"""

    def __init__(self , cloud_options , migrate_options, sys_adjust_overrides , skip_imaging=False, resume_upload=False, skip_upload=False , self_checks=False):
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
        """
        self.__adjustedSystemBackupSource = None
        self.__systemBackupSource = None
        self.__systemTransferTarget = None
        self.__systemAdjustOptions = None
        # the channel is underlying connection to transfer the data for system target
        self.__systemTransferChannel = None
        self.__systemMedia = None

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
       
        self.__resultingInstance = None

        self.__cloudName = self.__cloudOptions.getTargetCloud()

        #extra megabyte of additional size to store mbr, etc on the image
        self.__additionalMediaSize = 0x800*0x200

        
        
        #TODO: analyze both host and source systems
        if self.__migrateOptions.getHostOs() == "Windows":
            import Windows
            self.__windows = Windows.Windows()
            self.__runOnWindows = True
            self.__winSystemAdjustOptions = self.__windows.createSystemAdjustOptions(sys_adjust_overrides)
            self.__systemAdjustOptions = self.__winSystemAdjustOptions
            #self.__winSystemAdjustOptions

        if self.__cloudOptions.getTargetCloud() == "EC2":
            import S3UploadChannel
        if self.__cloudOptions.getTargetCloud() == "ElasticHosts":
            import EHUploadChannel

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
                    logging.info("Compatipility check failed")
                    return 

                logging.info("Checking the input config params")
                # 2) check the input parms
                if self.checkInputParams() == False:
                    logging.info("Parameter check failed")
                    return

                logging.info("Creating the transfer target")
                #NOTE: disk size should be min 1 mb larger (offset from mbr)
                # 3) create virtual image (transfer target)
                if self.createSystemTransferTarget() == False:
                    logging.info("Transfer target creation failed")
                    return
        
                # 4) gets system backup source
                logging.info("Initializing system copy source")
                if self.createSystemBackupSource() == False:
                    logging.info("Couldn't adjust the copy source")
                    return

                # 5) adjusts the system backup source
                logging.info("Adjusting the system copy parms")
                if self.adjustSystemBackupSource() == False:
                    logging.info("Couldn't adjust the copy parms")
                    return

                # 6) adjust transfer target to fit our needs
                logging.info("Adjusting the copy target")
                if self.adjustSystemBackupTarget() == False:
                    logging.info("Couldn't adjust the copy target")
                    return

                # 7) starts the data transfer
                logging.info("System copy started")
                if self.startSystemBackup() == False:
                    logging.info("System copy failed")
                    return
            else:
                logging.info(">>>>>>>>>>>>>>>>>>>>>>>>>> No system volumes specified for transfer") 

            # 8) TODO: instance creation should be distinct start()

            if self.__migrateOptions.getDataVolumes().__len__():
                logging.info(">>>>>>>>>>>>>>>>>>>>>>>>>> Migrating %d data volumes" , self.__migrateOptions.getDataVolumes().__len__()) 

                # 9) create data images\upload targets. that could be done in several ways
                logging.info("Initializing data transfer target")
                if self.createDataTransferTargets() == False:
                    logging.info("Transfer target creation failed")
                    return
        
                #TODO: create source adjust too

                # 10) gets system backup source
                logging.info("Initializing the system backup")
                if self.createDataBackupSources() == False:
                    logging.info("Couldn't adjust the copy source")
                    return

                # 11) adjust transfer target to fit our needs
                logging.info("Adjusting the copy target")
                if self.adjustDataBackupTarget() == False:
                    logging.info("Couldn't adjust the copy target")
                    return

                # 12) starts the data transfer
                logging.info("System copy started")
                if self.startDataBackup() == False:
                    logging.info("System copy failed")
                    return

            #
        
        # 8) adjust transfers after all?
        # Tha place of parallelism and asynchronousity is somewhere here
        # for now we just save the data here

        except Exception as ex:
            traceback.print_exception(sys.exc_info()[0] , sys.exc_info()[1] , sys.exc_info()[2]) 
            logging.error("!!!ERROR: Unexpected error occured")
            logging.error("!!!ERROR: " + str(ex))
            logging.error(traceback.format_exc())
            
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
            

    def checkSystemCompatibility(self):
        if self.__runOnWindows:
            sys_info = self.__windows.getSystemInfo()
            logging.info("System version: " + sys_info.getSystemVersionString() + " arch:" + hex(sys_info.getSystemArcheticture())) 
            if (sys_info.getSystemArcheticture() == sys_info.Archx8664 or sys_info.getSystemArcheticture() == sys_info.Archi386) and sys_info.getKernelVersion() >= sys_info.Win2003:
                return True
            logging.error("!!!ERROR: The configuration is not supported " + sys_info.getSystemVersionString() + " arch:" + hex(sys_info.getSystemArcheticture())) 
            logging.info("Windows 2003 , 2008R2 and 2012 Server are supported for now") 
        else: 
            logging.error("!!!ERROR: Non-Windows configs are not supported for now")
        return False

    #reads the config and generates appropriate handlers for the each step
    def generateStepHandlers(self):
        return True

    def generateSystemDataBackupSource(self):
        if self.__runOnWindows:
            return self.__windows.getSystemDataBackupSource()

    def generateDataBackupSource(self , volume):
        if self.__runOnWindows:
            return self.__windows.getDataBackupSource(volume)

    def createSystemBackupSource(self):
        """creates the system backup source, inits the system disk snapshot"""
        if self.__skipImaging:
            return True
        if self.__runOnWindows:
            import WindowsBackupSource
            import WindowsBackupAdjust
            self.__systemBackupSource = WindowsBackupSource.WindowsBackupSource()
            self.__adjustOption = WindowsBackupAdjust.WindowsBackupAdjust(self.__winSystemAdjustOptions , self.__windows.getVersion())

        self.__systemBackupSource.setBackupDataSource(self.generateSystemDataBackupSource())
        
        return True

    def adjustSystemBackupSource(self):
        """configures all adjusts needed for the system to be backed-up to transfer target"""
        if self.__skipImaging:
            return True

        self.__adjustedSystemBackupSource = AdjustedBackupSource.AdjustedBackupSource()
        self.__adjustedSystemBackupSource.setBackupSource(self.__systemBackupSource)

        self.__adjustOption.configureBackupAdjust(self.__systemBackupSource)
        # remove excluded files and dirs
        excludeddirs = self.__migrateOptions.getSystemVolumeConfig().getExcludedDirs()
        for excluded in excludeddirs:
            logging.info("Removing the file contents from directory " + str(excluded))
            fileenum = self.__systemBackupSource.getFileEnum(excluded)
            for file in fileenum:
                logging.debug("Contents of file " + str(file) + " is set to removal")
                self.__adjustOption.removeFile(str(file))
        
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
        return media

    def createTransferTarget(self , media , size , adjustoptions, newtarget = True, random_disk_id=True):
        """
        Internal call to make transfer target (disk volume abstraction) based on media and adjust options
        
        Args:
            media :ImageMedia - the media transfer target is based on
            size :long - the size of transfer target (volume)
            adjustoptions :SystemAdjustOptions - note really WindowsAdjustOptions is used. getNewMbrId() is declared there only
            newtarget: bool - if the target should be new one or old one could be used (not used, needed to signal the existing container should be re-created)
            random_disk_id: bool - if we use random disk id for the disk or get it from adjustoptions. Generally it's False for system disks but True for non-system ones
        """
        if newtarget:
            mbr = adjustoptions.getNewMbrId();
            if random_disk_id:
                mbr = int(random.randint(1, 0x0FFFFFFF))
            parser = SimpleDiskParser.SimpleDiskParser(SimpleDataTransferProto.SimpleDataTransferProto(media) , mbr_id = mbr , default_offset = self.__additionalMediaSize)
            return parser.createTransferTarget(size)
        return SimpleTransferTarget.SimpleTransferTarget( self.__additionalMediaSize , SimpleDataTransferProto.SimpleDataTransferProto(media) )
        

    def createSystemTransferTarget(self):
        """Creates transfer targets: disk images where the data should be transfered"""
        if self.__skipImaging == False or self.__skipUpload == False:
            self.__systemMedia = self.createImageMedia(self.__migrateOptions.getSystemImagePath() , self.__migrateOptions.getSystemImageSize() + self.__additionalMediaSize)          
            if self.__systemMedia == None:
                logging.error("!!!ERROR: Cannot create/open intermediate image (media) for an operation")
                return
            if self.__runOnWindows:
                #NOTE: the medai should be created nevertheless of the imaging done
                #so , calls are
                if self.__skipImaging == False:
                   self.__systemTransferTarget = self.createTransferTarget(self.__systemMedia , self.__migrateOptions.getSystemImageSize() , self.__winSystemAdjustOptions , random_disk_id=False)
            
            # move transfer creation to another function
            if self.__cloudName == "EC2":
                bucket = self.__cloudOptions.getCloudStorage()
                awskey = self.__cloudOptions.getCloudUser()
                awssecret = self.__cloudOptions.getCloudPass()
                awsregion = self.__cloudOptions.getRegion()
                import S3UploadChannel
                self.__systemTransferChannel = S3UploadChannel.S3UploadChannel(bucket, awskey, awssecret , self.__systemMedia.getMaxSize() , \
                                                      awsregion , self.__migrateOptions.getSystemVolumeConfig().getUploadPath() , self.__migrateOptions.getImageType() , self.__resumeUpload , self.__cloudOptions.getUploadChunkSize() )

            # ElasticHosts and other KVM options        
         
            # For the future: ! Note: make a better design here
            #if self.__migrateOptions.getImageType() == "raw" and self.__migrateOptions.getImagePlacement() == "local":
            #    self.__systemTransferTarget = self.createRawTranferTarget(self.__migrateOptions.getSystemImagePath() , self.__migrateOptions.getSystemImageSize()  , self.__winSystemAdjustOptions)
            #if self.__migrateOptions.getImageType() == "volume" and self.__migrateOptions.getImagePlacement() == "local":
            #    self.__systemTransferTarget = self.createRawTranferTarget(self.__migrateOptions.getSystemImagePath() , self.__migrateOptions.getSystemImageSize()  , self.__winSystemAdjustOptions)
        
               
            if self.__migrateOptions.getImageType() == "raw" and self.__migrateOptions.getImagePlacement() == "direct":
                if self.__cloudName == "ElasticHosts":
                    import EHUploadChannel
                    #directly from the snapshot to the server
                    return True
                    #TODO: make direct uploads
                    #self.__systemTransferChannel = EHUploadChannel.EHUploadChannel()
                
        
            if self.__cloudName == "ElasticHosts":
                #create the image first and then upload it
                import EHUploadChannel
                drive = self.__cloudOptions.getCloudStorage()
                userid = self.__cloudOptions.getCloudUser()
                apisecret = self.__cloudOptions.getCloudPass()
                region = self.__cloudOptions.getRegion()
                #TODO: move description (name seen by the user) to config!!!
                description = os.environ['COMPUTERNAME']+"-"+"system"+"-"+str(datetime.date.today())
                #Note: get upload path should be set to '' for the new downloads
                if self.__resumeUpload:
                    driveid = self.__migrateOptions.getSystemVolumeConfig().getUploadPath()
                else:
                    driveid = ''
                self.__systemTransferChannel = EHUploadChannel.EHUploadChannel(driveid , userid , apisecret , self.__systemMedia.getMaxSize() , region , description , self.__cloudOptions , self.__resumeUpload)
                
            # update the upload path in config in case it was changed or created by the channel
            uploadpath = self.__systemTransferChannel.getUploadPath()
            logging.debug("The upload channel path is: " + uploadpath)
            self.__migrateOptions.getSystemVolumeConfig().setUploadPath(uploadpath)

        return True
        
    def adjustSystemBackupTarget(self):
        # dunno really what could we do here
        return True

    def startSystemBackup(self):

        if self.__skipImaging:
            logging.info("\n>>>>>>>>>>>>>>>>> Skipping the system imaging\n") 
        else:
            #TODO: log and profile
            logging.info("\n>>>>>>>>>>>>>>>>> Started the system imaging\n") 
        
            #get data
            extents = self.__adjustedSystemBackupSource.getFilesBlockRange()
            
            #TODO: create kinda callbacks for transfers to monitor them
            self.__systemTransferTarget.transferRawData(extents)

            self.__systemTransferTarget.close()
            # TODO: better release options are needed
            if self.__runOnWindows:
                self.__windows.freeDataBackupSource(self.__systemBackupSource.getBackupDataSource())
                # extra testing
                if self.__selfChecks:
                    try:
                        logging.info("Making instance self-check") 
                        testmedia = self.__systemTransferTarget.getMedia()
                        import WindowsVhdMedia
                        if isinstance(testmedia, WindowsVhdMedia.WindowsVhdMedia):
                            testmedia.open()
                            mediapath = testmedia.getWindowsDevicePath()+"\\Partition1"
                            self.__adjustedSystemBackupSource.replacementSelfCheck(mediapath)
                            testmedia.close()
                    except Exception as e:
                        logging.warning("!Image self-check couldn't be done") 
                        logging.error("Exception occured = " + str(e)) 
                        logging.error(traceback.format_exc())
            
        
        # we save the config to reflect the image generated is ready. 
        #TODO: add the creation time here? or something alike? snapshot time too?
        self.__migrateOptions.getSystemVolumeConfig().saveConfig()
        
        imagesize = 0
        disksize = 0
        if self.__skipUpload:
            logging.info("\n>>>>>>>>>>>>>>>>> Skipping the system image upload\n") 
        else:
            logging.info("\n>>>>>>>>>>>>>>>>> Started the system image upload\n") 
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
  
        #TODO: add redeploy!

        #creating instance from the uploaded image
        # imagesize here is the size of image file. but getImageSize() is the size of initial volume
        self.generateInstance(self.__migrateOptions.getSystemVolumeConfig().getUploadId() , imagesize , disksize)

        return True

    def uploadImage(self , media, channel):
        imagesize = media.getImageSize()
        datasize = channel.getTransferChunkSize()
        logging.debug("Upload image of size " + str(imagesize) + " (full source disk data size is " + str(media.getMaxSize()) + " )") 
        dataplace = 0
        datasent = 0

        logging.info(">>>>>>>>>> Image size to upload: " + str(imagesize/1024/1024) + " MB")

        timestart = datetime.datetime.now()
        while dataplace < imagesize:
            percentcomplete = int(float(dataplace) / imagesize * 100);

            logmsg = ""
            if (datasent % 10 == 0):
                logmsg = " Progress:  " + str(percentcomplete) + "% - "
                if channel.getOverallDataSkipped():
                    logmsg = logmsg + str(int(channel.getOverallDataSkipped()/1024/1024)) + " MB are already in the cloud. "
                logmsg = logmsg + str(int(channel.getOverallDataTransfered()/1024/1024)) + " MB uploaded.   " ;

                #NOTE: this is a very rough estimate
                timenow = datetime.datetime.now()
                timedelta = datetime.timedelta()
                timedelta = timenow - timestart
                if dataplace and channel.getOverallDataTransfered():
                    secondsleft = int(float(imagesize - dataplace)/float(dataplace/timedelta.total_seconds()))
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

            data = media.readImageData(dataplace, datasize)
            if len(data) == 0:
                logging.warning("!Warning: the source archive has ended unexpectedly while uploading...");
                continue
            dataext = DataExtent.DataExtent(dataplace , len(data))
            dataplace = dataplace + len(data)
            dataext.setData(data)
            channel.uploadData(dataext)
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
        if self.__cloudName == "EC2":
            logging.info("Creating EC2 VM from the image stored at " + str(imageid))
            import EC2InstanceGenerator
            import EC2Instance
            awskey = self.__cloudOptions.getCloudUser()
            awssecret = self.__cloudOptions.getCloudPass()
            generator = EC2InstanceGenerator.EC2InstanceGenerator(self.__cloudOptions.getRegion())

            self.__resultingInstance = generator.makeInstanceFromImage(imageid, self.__cloudOptions , awskey, awssecret , self.__migrateOptions.getSystemImagePath() , imagesize , volumesize , self.__migrateOptions.getImageType())
        
        if self.__cloudName == "ElasticHosts":
            logging.info("Disk UUID " + imageid + " now contain your server image. Create a new server via ElasticHosts contol panel and attach the disk to ide0:0 port.")

        return True

    #TODO: make parameters optional
    # if they are not specified - load imageid
    def generateVolume(self , imageid , localimagepath , imagesize , volumesize):
        if self.__cloudName == "EC2":
            import EC2VolumeGenerator
            import EC2Instance
            import EC2Volume
            awskey = self.__cloudOptions.getCloudUser()
            awssecret = self.__cloudOptions.getCloudPass()
            generator = EC2VolumeGenerator.EC2VolumeGenerator(self.__cloudOptions.getRegion())

            instance = generator.makeVolumeFromImage(imageid, self.__cloudOptions , awskey, awssecret , localimagepath , imagesize , volumesize , self.__migrateOptions.getImageType())
        

        return True


    def createDataTransferTargets(self):

        if self.__skipImaging == False or self.__skipUpload == False:
            for volinfo in self.__migrateOptions.getDataVolumes():
                media = self.createImageMedia(volinfo.getImagePath() , volinfo.getImageSize() + self.__additionalMediaSize)          
                if media == None:
                    logging.error("!!!ERROR: Cannot create/open intermediate image (media) for an operation")
                    return False
                self.__dataMediaList[volinfo.getVolumePath()]=media
                if self.__runOnWindows:
                    #TODO: need kinda redisign the stuff related to system adjusts!
                    if self.__skipImaging == False:
                       self.__dataTransferTargetList[volinfo.getVolumePath()] = self.createTransferTarget(media , volinfo.getImageSize(), self.__winSystemAdjustOptions)
        
        if self.__skipUpload == False:
            if self.__cloudName == "EC2":
                bucket = self.__cloudOptions.getCloudStorage()
                awskey = self.__cloudOptions.getCloudUser()
                awssecret = self.__cloudOptions.getCloudPass()

                # here we should get system bucket and the system key from the config
                # keyname should be associated with the volume by the config program
                import S3UploadChannel 
                for volinfo in self.__migrateOptions.getDataVolumes():
                    self.__dataChannelList[volinfo.getVolumePath()] = S3UploadChannel.S3UploadChannel(bucket, awskey, awssecret , self.__dataMediaList[volinfo.getVolumePath()].getMaxSize() , self.__cloudOptions.getRegion() , volinfo.getUploadPath() , self.__migrateOptions.getImageType() , self.__resumeUpload)

            # ElasticHosts part
            if self.__migrateOptions.getImageType() == "raw" and self.__migrateOptions.getImagePlacement() == "direct":
                if self.__cloudName == "ElasticHosts":
                    import EHUploadChannel
                    #directly from the snapshot to the server
                    return True
                    #TODO: make direct uploads
                    #self.__systemTransferChannel = EHUploadChannel.EHUploadChannel()
                
        
            if self.__cloudName == "ElasticHosts":
                #create the image first and then upload it
                import EHUploadChannel
                drive = self.__cloudOptions.getCloudStorage()
                userid = self.__cloudOptions.getCloudUser()
                apisecret = self.__cloudOptions.getCloudPass()
                region = self.__cloudOptions.getRegion()
                #Note: get upload path should be set to '' for the new uploads
                description = os.environ['COMPUTERNAME']+"-"+"data"+"-"+str(datetime.date.today())
                for volinfo in self.__migrateOptions.getDataVolumes():
                    if self.__resumeUpload:
                        driveid = volinfo.getUploadPath()
                    else:
                        driveid = ''
                    self.__dataChannelList[volinfo.getVolumePath()] = EHUploadChannel.EHUploadChannel(driveid , userid , apisecret , self.__dataMediaList[volinfo.getVolumePath()].getMaxSize() , region , description , self.__cloudOptions, self.__resumeUpload)
                  
            # update the upload path in config in case it was changed or created by the channel
            for volinfo in self.__migrateOptions.getDataVolumes():
                uploadpath = self.__dataChannelList[volinfo.getVolumePath()].getUploadPath()
                logging.debug("The upload channel path is: " + uploadpath)
                volinfo.setUploadPath(uploadpath)

        return True
        
    def createDataBackupSources(self):
        if self.__skipImaging:
            return True
        if self.__runOnWindows:
            import WindowsBackupSource
            import WindowsBackupAdjust
            for volinfo in self.__migrateOptions.getDataVolumes():
                backupsource = WindowsBackupSource.WindowsBackupSource()
                backupsource.setBackupDataSource(self.generateDataBackupSource(volinfo.getVolumePath()))
                self.__dataBackupSourceList[volinfo.getVolumePath()] = backupsource
            
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
                    self.__windows.freeDataBackupSource(self.__dataBackupSourceList[volinfo.getVolumePath()].getBackupDataSource())

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
            self.generateVolume(volinfo.getUploadId() , volinfo.getImagePath() , mediaimagesize , disksize )

       
        return True
