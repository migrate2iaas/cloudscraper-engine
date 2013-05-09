
import ImageMedia
import TransferTarget
import DataExtent

# this class may be used for simple rw-operations. it just writes to media with offset decorate it with any other transfer targets to obtain customazible functionality
class SimpleTransferTarget(TransferTarget.TransferTarget):
    """Class representing simple transfer target with read and write operations only"""

    def __init__(self , offset , mediaproto):
        self.__offset = offset
        self.__mediaProto = mediaproto

    # writes the file
    # need file metadata\permissions too
    def transferFile(self , fileToBackup):
        raise NotImplementedError

    # transfers file data only, no metadata should be written
    def transferFileData(self, fileName, fileExtentsData):
        raise NotImplementedError

    # transfers file data only
    def transferRawData(self, volExtents):
        for extent in volExtents:
            displaced_ext = DataExtent.DataExtent(extent.getStart() + self.__offset , extent.getSize())
            displaced_ext.setData(extent.getData())
            self.__mediaProto.writeData(displaced_ext) 

    # transfers raw metadata, it should be precached
    def transferRawMetadata(self, volExtents):
        return self.transferRawData(volExtents) 

    #deletes file transfered
    def deleteFileTransfer(self , filename):
        raise NotImplementedError

    #cancels the transfer and deletes the transfer target
    def cancelTransfer(self):
        raise NotImplementedError

    def getMedia(self):
        return self.__mediaProto.getMedia()

    def close(self):
        return self.__mediaProto.getMedia().release()