import AdjustedBackupSource
import logging
import traceback
import sys
import os
import stat
import DataExtent

import MigrateConfig
import CloudConfig
import SystemAdjustOptions

class Migrator(object):
    """Here I came to the trap of all products: make kinda place with function DO-EVERYTHING-I-WANT"""


    def __init__2(self , migrateOptions):
        self.__adjustedSystemBackupSource = None
        self.__systemBackupSource = None
        self.__adjustOption = None
        self.__migrateOptions = migrateOptions
        self.__cloudOptions = migrateOptions
        self.__systemTransferTarget = None
        self.__systemAdjustOptions = None
        
        self.__runOnWindows = False

        if self.__migrateOptions.getHostOs() == "Windows":
            import Windows
            self.__windows = Windows.Windows()
            self.__runOnWindows = True
        if self.__cloudOptions.getTargetCloud() == "EC2":
            import S3UploadChannel
            self.__cloudName = self.__migrateOptions.getTargetCloud()

    def __init__(self , cloudOptions , migrateOptions, sysAdjustOptions , skipImaging=False, resumeUpload=False, skipUpload=False):
        self.__adjustedSystemBackupSource = None
        self.__systemBackupSource = None
        self.__systemTransferTarget = None
        self.__systemAdjustOptions = sysAdjustOptions
        # the channel is underlying connection to transfer the data for system target
        self.__systemTransferChannel = None

        self.__adjustOption = None
        self.__migrateOptions = migrateOptions
        self.__cloudOptions = cloudOptions
        
        self.__dataBackupSourceList = dict() # the key is data volume device path
        self.__dataTransferTargetList = dict()
        self.__dataChannelList = dict()
        
        self.__runOnWindows = False

        self.__skipImaging = skipImaging
        self.__skipUpload = skipUpload
        self.__resumeUpload = resumeUpload

        #TODO: analyze both host and source systems
        if self.__migrateOptions.getHostOs() == "Windows":
            import Windows
            self.__windows = Windows.Windows()
            self.__runOnWindows = True
            self.__winSystemAdjustOptions = None
        if self.__cloudOptions.getTargetCloud() == "EC2":
            import S3UploadChannel
            self.__cloudName = self.__cloudOptions.getTargetCloud()

    # runs full scenario from the start to the upload
    def runFullScenario(self):

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
                logging.info(">>>>>>>>>>>>>>>>>>>>>>>>>> No system volumes specified for transfer");

            # 8) TODO: instance creation should be distinct start()

            if self.__migrateOptions.getDataVolumes().__len__():
                logging.info(">>>>>>>>>>>>>>>>>>>>>>>>>> Migrating %d data volumes" , self.__migrateOptions.getDataVolumes().__len__());

                # 9) create data images\upload targets. that could be done in several ways
                logging.info("Initializing data transfer target")
                if self.createDataTransferTargets() == False:
                    logging.info("Transfer target creation failed")
                    return
        
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
            traceback.print_exception(sys.exc_info()[0] , sys.exc_info()[1] , sys.exc_info()[2]);
            logging.error("!!!ERROR: Unexpected error occured")
            logging.error("!!!ERROR: " + str(ex))
            logging.error(traceback.format_exc())
            
        finally:
            for channel in self.__dataChannelList.values():
                channel.close()
            if self.__systemTransferChannel:
                self.__systemTransferChannel.close()
         
            
            

        # TODO: catch and free resources here! registry, files, vhds, snapshots.
        # use context manager for that
        return

    def generateStepHandlers(self):
        # kinda of config factory in here
        # just schamatic:
       
        return True
            


    def checkSystemCompatibility(self):
        if self.__runOnWindows:
            SysInfo = self.__windows.getSystemInfo()
            if SysInfo.getSystemArcheticture() == SysInfo.Archx8664 and SysInfo.getKernelVersion() >= SysInfo.Win2008R2:
                return True
            logging.error("!!!ERROR: The configuration is not supported " + SysInfo.getSystemVersionString() + " arch:" + hex(SysInfo.getSystemArcheticture()));
            logging.info("Windows 2008R2 and 2012 Server are supported for now");
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
        if self.__skipImaging:
            return True
        if self.__runOnWindows:
            import WindowsBackupSource
            import WindowsBackupAdjust
            self.__systemBackupSource = WindowsBackupSource.WindowsBackupSource()
            self.__adjustOption = WindowsBackupAdjust.WindowsBackupAdjust(self.__winSystemAdjustOptions)

        self.__systemBackupSource.setBackupDataSource(self.generateSystemDataBackupSource())
        return True

    def adjustSystemBackupSource(self):
        
        if self.__skipImaging:
            return True

        self.__adjustedSystemBackupSource = AdjustedBackupSource.AdjustedBackupSource()
        self.__adjustedSystemBackupSource.setBackupSource(self.__systemBackupSource)

        self.__adjustOption.configureBackupAdjust(self.__systemBackupSource)

        self.__adjustedSystemBackupSource.setAdjustOption(self.__adjustOption)

        return True

    def createSystemTransferTarget(self):
        
        if self.__runOnWindows:
            #TODO: need kinda redisign the stuff related to system adjusts!
            self.__winSystemAdjustOptions = self.__windows.createSystemAdjustOptions()
            self.__winSystemAdjustOptions.setSysDiskType(self.__systemAdjustOptions.getSysDiskType())
            #self.__systemAdjustOptions.loadConfig(self.__migrateOptions.getSystemConfig())
            
            if self.__skipImaging == False:
                if self.__migrateOptions.getImageType() == "VHD" and self.__migrateOptions.getImagePlacement() == "local" and self.__windows.getVersion() >= self.__windows.getSystemInfo().Win2008R2:
                   #TODO: create max VHD size parm also
                   self.__systemTransferTarget = self.__windows.createVhdTransferTarget(self.__migrateOptions.getSystemImagePath() , self.__migrateOptions.getSystemImageSize()  , self.__winSystemAdjustOptions)
                else:
                    #TODO: be more discriptive
                    logging.error("!!!ERROR: Bad image options!");
                    return False
        if self.__cloudName == "EC2":
            bucket = self.__cloudOptions.getCloudStorage()
            awskey = self.__cloudOptions.getCloudUser()
            awssecret = self.__cloudOptions.getCloudPass()

            # here we should get system bucket and the system key from the config
            # keyname should be associated with the volume by the config program

            import S3UploadChannel
            self.__systemTransferChannel = S3UploadChannel.S3UploadChannel(bucket, awskey, awssecret , self.__migrateOptions.getSystemImageSize() , self.__cloudOptions.getRegion() , self.__migrateOptions.getSystemVolumeConfig().getUploadPath() , self.__migrateOptions.getImageType() , self.__resumeUpload)
            

        return True

    def adjustSystemBackupTarget(self):
        # dunno really what could we do here
        return True

    def startSystemBackup(self):

        if self.__skipImaging:
            logging.info("\n>>>>>>>>>>>>>>>>> Skipping the system imaging\n");
        else:
            #TODO: log and profile
            logging.info("\n>>>>>>>>>>>>>>>>> Started the system imaging\n");
        
            #get data
            extents = self.__adjustedSystemBackupSource.getFilesBlockRange()
        
            #TODO: create kinda callbacks for transfers to monitor them
            #write,
            self.__systemTransferTarget.TransferRawData(extents)

            #TODO: quick and dirty workaround, choose something better!
            if self.__runOnWindows:
               self.__windows.closeMedia()

            # we save the config to reflect the image generated is ready. 
            #TODO: add the creation time here? or something alike? snapshot time too?
            self.__migrateOptions.getSystemVolumeConfig().saveConfig()
        

        filesize = os.stat(self.__migrateOptions.getSystemImagePath()).st_size
        #TODO: should be 10  mb in amazon impl
        file = open(self.__migrateOptions.getSystemImagePath() , "rb")

        if self.__skipUpload:
            logging.info("\n>>>>>>>>>>>>>>>>> Skipping the system image upload\n");
        else:
            logging.info("\n>>>>>>>>>>>>>>>>> Started the system image upload\n");

            datasize = self.__systemTransferChannel.getTransferChunkSize()#mb
            dataplace = 0
            datasent = 0
            while 1:
                try:
                    data = file.read(datasize)
                except EOFError:
                    break
                if len(data) == 0:
                    break
                dataext = DataExtent.DataExtent(dataplace , len(data))
                dataplace = dataplace + len(data)
                dataext.setData(data)
                self.__systemTransferChannel.uploadData(dataext)
                datasent = datasent + 1
                if (datasent % 50 == 0):
                    logmsg = "% " + str(datasent*datasize/1024/1024)+ " of "+ str(int(filesize/1024/1024)) + " MB of image data processed. "
                    if self.__systemTransferChannel.getOverallDataSkipped():
                        logmsg = logmsg + str(int(self.__systemTransferChannel.getOverallDataSkipped()/1024/1024)) + " MB are already in the cloud. "
                    logging.info( logmsg + str(int(self.__systemTransferChannel.getOverallDataTransfered()/1024/1024)) + " MB uploaded."  )

        
            self.__systemTransferChannel.waitTillUploadComplete()
            logging.info("Preparing the image uploaded for cloud use")
            imageid = self.__systemTransferChannel.confirm()
            self.__systemTransferChannel.close()
            self.__migrateOptions.getSystemVolumeConfig().setUploadId(imageid)
            self.__migrateOptions.getSystemVolumeConfig().saveConfig()
        #TODO: somehow the image-id should be saved thus it could be loaded in skip upload scenario
        #move it somehwere else
        logging.info("Creating VM from the image stored at " + str(self.__migrateOptions.getSystemVolumeConfig().getUploadId()))
        self.generateInstance(self.__migrateOptions.getSystemVolumeConfig().getUploadId())

        return True

    def checkInputParams(self):
        return True

    def generateInstance(self , imageid):
        if self.__cloudName == "EC2":
            import EC2InstanceGenerator
            import EC2Instance
            awskey = self.__cloudOptions.getCloudUser()
            awssecret = self.__cloudOptions.getCloudPass()
            generator = EC2InstanceGenerator.EC2InstanceGenerator(self.__cloudOptions.getRegion())

            instance = generator.makeInstanceFromImage(imageid, self.__cloudOptions , awskey, awssecret , self.__migrateOptions.getSystemImagePath())
            
            
        return True

    def generateVolume(self , imageid):
        if self.__cloudName == "EC2":
            import EC2VolumeGenerator
            import EC2Instance
            import EC2Volume
            awskey = self.__cloudOptions.getCloudUser()
            awssecret = self.__cloudOptions.getCloudPass()
            generator = EC2VolumeGenerator.EC2VolumeGenerator(self.__cloudOptions.getRegion())

            instance = generator.makeInstanceFromImage(imageid, self.__cloudOptions , awskey, awssecret , self.__migrateOptions.getSystemImagePath())
        

        return True


    def createDataTransferTargets(self):
        if self.__runOnWindows:  
            if self.__skipImaging == False:
                for volinfo in self.__migrateOptions.getDataVolumes():
                    if self.__migrateOptions.getImageType() == "VHD" and self.__migrateOptions.getImagePlacement() == "local" and self.__windows.getVersion() >= self.__windows.getSystemInfo().Win2008R2:
                       #TODO: create max VHD size parm also
                       #TODO: create adjust options should be moved somewhere else, needs some archeticture
                       self.__dataTransferTargetList[volinfo.getVolumePath()] = self.__windows.createVhdTransferTarget(volinfo.getImagePath() , volinfo.getImageSize()  , self.__windows.createSystemAdjustOptions())
                    else:
                        #TODO: be more discriptive
                        logging.error("!!!ERROR: Bad image options!");
                        return False
        if self.__cloudName == "EC2":
            bucket = self.__cloudOptions.getCloudStorage()
            awskey = self.__cloudOptions.getCloudUser()
            awssecret = self.__cloudOptions.getCloudPass()

            # here we should get system bucket and the system key from the config
            # keyname should be associated with the volume by the config program
            import S3UploadChannel 
            for volinfo in self.__migrateOptions.getDataVolumes():
                self.__dataChannelList[volinfo.getVolumePath()]= S3UploadChannel.S3UploadChannel(bucket, awskey, awssecret , volinfo.getImageSize() , self.__cloudOptions.getRegion() , volinfo.getUploadPath() , self.__migrateOptions.getImageType() , self.__resumeUpload)
            

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
            logging.info("\n>>>>>>>>>>>>>>>>> Skipping the data volume imaging\n");
        else:
            #TODO: log and profile
            logging.info("\n>>>>>>>>>>>>>>>>> Started the data volune imaging\n");
            for volinfo in self.__migrateOptions.getDataVolumes():
                #get data
                extents = self.__dataBackupSourceList[volinfo.getVolumePath()].getFilesBlockRange()
        
                #TODO: create kinda callbacks for transfers to monitor them
                #write,
                self.__dataTransferTargetList[volinfo.getVolumePath()].TransferRawData(extents)

                #TODO: quick and dirty workaround, choose something better!
                if self.__runOnWindows:
                   self.__windows.closeMedia()

                # we save the config to reflect the image generated is ready. 
                #TODO: add the creation time here? or something alike? snapshot time too?
                volinfo.saveConfig()
        
        for volinfo in self.__migrateOptions.getDataVolumes():

            filesize = os.stat(volinfo.getImagePath()).st_size
            #TODO: should be 10  mb in amazon impl
            file = open(volinfo.getImagePath() , "rb")

            if self.__skipUpload:
                logging.info("\n>>>>>>>>>>>>>>>>> Skipping the data image upload\n");
            else:
                logging.info("\n>>>>>>>>>>>>>>>>> Started the data image upload\n");

                channel = self.__dataChannelList[volinfo.getVolumePath()]

                datasize =  channel.getTransferChunkSize()#mb
                dataplace = 0
                datasent = 0
                while 1:
                    try:
                        data = file.read(datasize)
                    except EOFError:
                        break
                    if len(data) == 0:
                        break
                    dataext = DataExtent.DataExtent(dataplace , len(data))
                    dataplace = dataplace + len(data)
                    dataext.setData(data)
                    channel.uploadData(dataext)
                    datasent = datasent + 1
                    if (datasent % 50 == 0):
                        logmsg = "% " + str(datasent*datasize/1024/1024)+ " of "+ str(int(filesize/1024/1024)) + " MB of image data processed. "
                        if  channel.getOverallDataSkipped():
                            logmsg = logmsg + str(int(channel.getOverallDataSkipped()/1024/1024)) + " MB are already in the cloud. "
                        logging.info( logmsg + str(int(channel.getOverallDataTransfered()/1024/1024)) + " MB uploaded."  )

        
                channel.waitTillUploadComplete()
                logging.info("Preparing the image uploaded for cloud use")
                imageid =  channel.confirm()
                channel.close()
                # TODO: add volumes upload
                volinfo.setUploadId(imageid)
                volinfo.saveConfig()
            #TODO: somehow the image-id should be saved thus it could be loaded in skip upload scenario
            #move it somehwere else
            logging.info("Creating volume from the image stored at " + str(self.__migrateOptions.getSystemVolumeConfig().getUploadId()))
            self.generateVolume(self.__migrateOptions.getSystemVolumeConfig().getUploadId())

       
        return True
