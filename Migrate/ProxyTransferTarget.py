
import ImageMedia
import TransferTarget
import DataExtent
import logging
import struct

# this class may be used for simple rw-operations. it just writes to media with offset decorate it with any other transfer targets to obtain customazible functionality
class ProxyTransferTarget(TransferTarget.TransferTarget):
    """Class representing simple transfer target with read and write operations only"""

    def __init__(self , mediaproto):
        self.__mediaProto = mediaproto
        self.__closed = False

    # writes the file
    # need file metadata\permissions too
    def transferFile(self , fileToBackup):
        raise NotImplementedError

    # transfers file data only, no metadata should be written
    def transferFileData(self, fileName, fileExtentsData):
        raise NotImplementedError

    # transfers file data only
    def transferRawData(self, volExtents):
        if self.__closed == True:
            logging.warning("The transfer target is closed, doing nothing")
            return
        
        extentswritten = 0
        
        for extent in volExtents:
            self.__mediaProto.writeData(extent) 
            extentswritten = extentswritten + 1
            if ( extentswritten  % 100 == 0):
                #TODO: make better data approximation
                logging.info("% " + str(extentswritten) + " of " + str(len(volExtents)) + " original disk data have been transferred to the image ("+ str(extentswritten*100/len(volExtents)) +"%)" )
        
        logging.info("%  Disk image has been successfully created (100%)" )


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
        if self.__closed == False:
            if self.__mediaProto.getMedia().release():
                self.__closed = True
                return True
        else:
            logging.warning("Attempt to close target that was already closed")
        return False