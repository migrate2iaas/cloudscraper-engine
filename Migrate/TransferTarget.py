
class TransferTarget(object):
    """Abstract class representing backup transfer target"""

    # TODO: change coding styles

    # writes the file
    # need file metadata\permissions too
    def TransferFile(self , fileToBackup):
        raise NotImplementedError

    # transfers file data only, no metadata should be written
    def TransferFileData(self, fileName, fileExtentsData):
        raise NotImplementedError

    # transfers file data only
    def TransferRawData(self, volExtent):
        raise NotImplementedError

    # transfers raw metadata, it should be precached
    def TransferRawMetadata(self, volExtent):
        raise NotImplementedError

    #deletes file transfered
    def DeleteFileTransfer(self , filename):
        raise NotImplementedError

    #cancels the transfer and deletes the transfer target
    def cancelTransfer(self):
        raise NotImplementedError

    # TODO: think of better implementation