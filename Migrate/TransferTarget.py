
class TransferTarget(object):
    """Abstract class representing backup transfer target"""

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
    def DeleteFileTransfer(self):
        raise NotImplementedError

    # TODO: think of better implementation