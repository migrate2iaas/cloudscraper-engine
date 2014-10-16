# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


class TransferTarget(object):
    """Abstract class representing backup transfer target"""

    # TODO: change coding styles

    def transferFile(self , fileToBackup):
        """
         writes the file
         need file metadata\permissions too
        """
        raise NotImplementedError

    def transferFileData(self, fileName, fileExtentsData):
        """
            transfers file data only, no metadata should be written
        """
        raise NotImplementedError


    def transferRawData(self, volExtents):
        """    transfers file data only"""
        raise NotImplementedError

    def transferRawMetadata(self, volExtents):
        """transfers raw metadata, it should be precached"""
        raise NotImplementedError

    def deleteFileTransfer(self , filename):
        """deletes file transfered"""
        raise NotImplementedError

    #cancels the transfer and deletes the transfer target
    def cancelTransfer(self):
        raise NotImplementedError

    def getMedia(self):
        raise NotImplementedError

    def close(self):
        return None