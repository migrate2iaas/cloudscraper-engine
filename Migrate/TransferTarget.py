# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


class TransferTarget(object):
    """Abstract class representing backup transfer target"""

    # TODO: change coding styles

    # writes the file
    # need file metadata\permissions too
    def transferFile(self , fileToBackup):
        raise NotImplementedError

    # transfers file data only, no metadata should be written
    def transferFileData(self, fileName, fileExtentsData):
        raise NotImplementedError

    # transfers file data only
    def transferRawData(self, volExtent):
        raise NotImplementedError

    # transfers raw metadata, it should be precached
    def transferRawMetadata(self, volExtent):
        raise NotImplementedError

    #deletes file transfered
    def deleteFileTransfer(self , filename):
        raise NotImplementedError

    #cancels the transfer and deletes the transfer target
    def cancelTransfer(self):
        raise NotImplementedError

    # TODO: think of better implementation