# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

class FileToBackup(object):
    """Abstract class of file description to backup incl its name and changed range"""

    # getters:

    def getName(self):
        raise NotImplementedError

    def getDestPath(self):
        raise NotImplementedError

    def getSourcePath(self):
        raise NotImplementedError

    def getBackupSource(self):
        raise NotImplementedError

    def getTransferDest(self):
        raise NotImplementedError

    def getChangedExtents(self):
        raise NotImplementedError

    #setters:

    def setDestPath(self, path):
        raise NotImplementedError

    def setTransferDest(self, dest):
        raise NotImplementedError

    #returns data read
    def readData(self,extent):
        raise NotImplementedError

