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
        self.__adjustedBackupSource = None
        self.__backupSource = None
        self.__adjustOption = None
        self.__migrateOptions = migrateOptions
        self.__cloudOptions = migrateOptions
        self.__transferTarget = None
        self.__systemAdjustOptions = None
        
        self.__runOnWindows = False

        if self.__migrateOptions.getHostOs() == "Windows":
            import Windows
            self.__windows = Windows.Windows()
            self.__runOnWindows = True
        if self.__cloudOptions.getTargetCloud() == "EC2":
            import S3UploadChannel
            self.__cloudName = self.__migrateOptions.getTargetCloud()

    def __init__(self , cloudOptions , migrateOptions, sysAdjustOptions , skipImaging=False, skipUpload=False, resumeUpload=False):
        self.__adjustedBackupSource = None
        self.__backupSource = None
        self.__adjustOption = None
        self.__migrateOptions = migrateOptions
        self.__cloudOptions = cloudOptions
        self.__transferTarget = None
        self.__systemAdjustOptions = sysAdjustOptions
        
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
        
        # 8) adjust transfers after all?
        # Tha place of parallelism and asynchronousity is somewhere here
        # for now we just save the data here

        except Exception as ex:
            print "Unknown Error!"
            print type(ex)     # the exception instance
            print ex.args      # arguments stored in .args
            print ex
            traceback.print_exception(sys.exc_info()[0] , sys.exc_info()[1] , sys.exc_info()[2]);
            logging.error("!!!ERROR: Unexpected error occured")
            logging.error(ex)
            logging.error(traceback.format_exc())
            
         
            
            

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
        logging.error("!!!ERROR: Non-Windows configs are not supported for now")
        return False

    #reads the config and generates appropriate handlers for the each step
    def generateStepHandlers(self):
        return True

    def generateSystemDataBackupSource(self):
        if self.__runOnWindows:
            return self.__windows.getSystemDataBackupSource()

    def createSystemBackupSource(self):
        if self.__skipImaging:
            return True
        if self.__runOnWindows:
            import WindowsBackupSource
            import WindowsBackupAdjust
            self.__backupSource = WindowsBackupSource.WindowsBackupSource()
            self.__adjustOption = WindowsBackupAdjust.WindowsBackupAdjust(self.__winSystemAdjustOptions)

        self.__backupSource.setBackupDataSource(self.generateSystemDataBackupSource())
        return True

    def adjustSystemBackupSource(self):
        self.__adjustedBackupSource = AdjustedBackupSource.AdjustedBackupSource()
        self.__adjustedBackupSource.setBackupSource(self.__backupSource)

        self.__adjustOption.configureBackupAdjust(self.__backupSource)

        self.__adjustedBackupSource.setAdjustOption(self.__adjustOption)

        return True

    def createSystemTransferTarget(self):
        
        if self.__runOnWindows:
            #TODO: need kinda redisign the stuff related to system adjusts!
            self.__winSystemAdjustOptions = self.__windows.createSystemAdjustOptions()
            self.__winSystemAdjustOptions.setSysDiskType(self.__systemAdjustOptions.getSysDiskType())
            #self.__systemAdjustOptions.loadConfig(self.__migrateOptions.getSystemConfig())
            

            if self.__migrateOptions.getImageType() == "VHD" and self.__migrateOptions.getImagePlacement() == "local" and self.__windows.getVersion() >= self.__windows.getSystemInfo().Win2008R2 and self.__skipImaging == False:
               #TODO: create max VHD size parm also
               self.__transferTarget = self.__windows.createVhdTransferTarget(self.__migrateOptions.getSystemImagePath() , self.__migrateOptions.getSystemImageSize()  , self.__winSystemAdjustOptions)
            else:
                #TODO: be more discriptive
                logging.error("!!!ERROR: Bad image options!");
                return False
        if self.__cloudName == "EC2":
            bucket = self.__cloudOptions.getCloudStorage()
            awskey = self.__cloudOptions.getCloudUser()
            awssecret = self.__cloudOptions.getCloudPass()
            import S3UploadChannel
            self.__transferChannel = S3UploadChannel.S3UploadChannel(bucket, awskey, awssecret , self.__migrateOptions.getSystemImageSize() , self.__cloudOptions.getRegion() , None , self.__migrateOptions.getImageType())

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
            extents = self.__adjustedBackupSource.getFilesBlockRange()
        
            #TODO: create kinda callbacks for transfers to monitor them
            #write,
            self.__transferTarget.TransferRawData(extents)

            #TODO: quick and dirty workaround, choose something better!
            if self.__runOnWindows:
               self.__windows.closeMedia()

        filesize = os.stat(self.__migrateOptions.getSystemImagePath()).st_size
        #TODO: should be 10  mb in amazon impl
        file = open(self.__migrateOptions.getSystemImagePath() , "rb")
        
        logging.info("\n>>>>>>>>>>>>>>>>> Started the system image upload\n");

        datasize = 10*1024*1024 #mb
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
            self.__transferChannel.uploadData(dataext)
            datasent = datasent + 1
            if (datasent % 100 == 10):
                logging.info("% " + str(datasent*datasize/1024/1024)+ " of "+ str(int(filesize/1024/1024)) + " MB sent to " + self.__cloudName)

        
        self.__transferChannel.waitTillUploadComplete()
        logging.info("Preparing the image uploaded for cloud use")
        imageid = self.__transferChannel.confirm()
        imageid = self.__transferChannel.close()

        logging.info("Creating VM from the image...")
        self.generateInstance(imageid)

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
