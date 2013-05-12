
import ImageMedia
import TransferTarget
import DataExtent
import logging
import struct

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
        # we change bootsector so the sysem could boot from it
        #TODO: move to specialized (NTFS) transfer target
        bootsector = DataExtent.DataExtent(0,512)
        extentswritten = 0
        
        logging.debug("Found the boot sector, altering it a bit")
        for extent in volExtents:
            #special handling for a boot options
            if bootsector in extent:
                logging.debug("Altering the $boot extent in " + str(extent) )
                if extent.getStart() == 0:
                    data = extent.getData()
                    # the only place it could boot from is standard offset of 1mb
                    offsetsectors = int(self.__offset/512)
                    data = data[:0x1c] + struct.pack('=i',offsetsectors) + data[0x20:]
                displaced_ext = DataExtent.DataExtent(extent.getStart() + self.__offset , extent.getSize())
                displaced_ext.setData(data)
                self.__mediaProto.writeData(displaced_ext) 
                continue                    
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