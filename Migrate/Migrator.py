import AdjustedBackupSource
import logging
import traceback
import sys

class Migrator(object):
    """Here I came to the trap of all products: make kinda place with function DO-EVERYTHING-I-WANT"""

    def __init__(self , userOptions):
        self.__adjustedBackupSource = None
        self.__backupSource = None
        self.__adjustOption = None
        self.__migrateOptions = userOptions
        self.__transferTarget = None
        self.__systemAdjustOptions = None
        
        self.__runOnWindows = True

        if self.__migrateOptions.getHostOs() == "Windows":
            import Windows
            self.__windows = Windows.Windows()
            self.__runOnWindows = True

    # runs full scenario from the start to the upload
    def runFullScenario(self):

        try:
            logging.info("GeneratingmMigration process step handlers")
            # 0) check the input action config: what to do
            self.generateStepHandlers()
        
            logging.info("Checking system compatibility")
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
            logging.error("Unexpected error occured")
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
        return

    #reads the config and generates appropriate handlers for the each step
    def generateStepHandlers(self):
        return True

    def generateSystemDataBackupSource(self):
        if self.__runOnWindows:
            return self.__windows.getSystemDataBackupSource()

    def createSystemBackupSource(self):
        if self.__runOnWindows:
            import WindowsBackupSource
            import WindowsBackupAdjust
            self.__backupSource = WindowsBackupSource.WindowsBackupSource()
            self.__adjustOption = WindowsBackupAdjust.WindowsBackupAdjust(self.__systemAdjustOptions)

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
            self.__systemAdjustOptions = self.__windows.createSystemAdjustOptions()
            self.__systemAdjustOptions.loadConfig(self.__migrateOptions.getSystemConfig())

            if self.__migrateOptions.getImageType() == "vhd" and self.__migrateOptions.getImagePlacement() == "local" and self.__windows.getVersion() >= self.__windows.Win2008R2:
               self.__transferTarget = self.__windows.createVhdTransferTarget(self.__migrateOptions.getSystemImagePath() , self.__migrateOptions.getSystemImageSize()  , self.__systemAdjustOptions)
            else:
                #TODO: log
                return False
        return True

    def adjustSystemBackupTarget(self):
        # dunno really what could we do here
        return True

    def startSystemBackup(self):

        #TODO: log and profile

        #get data
        extents = self.__adjustedBackupSource.getFilesBlockRange()
        #write 
        self.__transferTarget.TransferRawData(extents)

        return True

    def checkInputParams(self):
        return True