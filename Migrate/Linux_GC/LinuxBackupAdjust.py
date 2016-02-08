import BackupAdjust

class LinuxBackupAdjust(BackupAdjust.BackupAdjust):
    """adjusting linux prior to doing migration"""

    def __init__(self):
        super(LinuxBackupAdjust, self).__init__()

    
    def configureBackupAdjust(self , backupSource, volumes):
        """
            here we should set the pathes for the backup source
            this function is called for the system volume only
        """

        # remove exclude path from here    

        pass


     