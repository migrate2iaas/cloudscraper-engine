# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

class BackupSource(object):
    """Class representing abstract Backup source"""

    # gets files enumerator
    # iterable of FileToBackup objects
    def getFileEnum(self):
        return

    # gets block range for range of files
    def getFilesBlockRange(self):
        return

    # gets block range for file specified
    def getFileBlockRange(self, filename):
        return

    # sets the data source
    def setBackupDataSource(self , dataSource):
        return

    # gets the data source
    def getBackupDataSource(self):
        return 

    #TODO: adds metadata blocks also