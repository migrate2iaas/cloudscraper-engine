import BackupAdjust

class LinuxBackupAdjust(BackupAdjust.BackupAdjust):
    """adjusting linux prior to doing migration"""

    def __init__(self):
        super(LinuxBackupAdjust, self).__init__()

    def configureBackupAdjust(self , backupSource):
        # TODO: add service stop here

        # TODO: add sync here
        

        # TODO: add service restart later
        # add generic post-backup code
        pass


     