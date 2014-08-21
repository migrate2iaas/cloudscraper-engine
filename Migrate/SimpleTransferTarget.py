
import ImageMedia
import TransferTarget
import DataExtent
import logging
import struct

# this class may be used for simple rw-operations. it just writes to media with offset decorate it with any other transfer targets to obtain customazible functionality
class SimpleTransferTarget(TransferTarget.TransferTarget):
    """Class representing simple transfer target with read and write operations only"""

    def __init__(self , offset , mediaproto, fix_nt_boot=True):
        self.__offset = offset
        self.__mediaProto = mediaproto
        self.__closed = False
        self.__fixNtBoot = fix_nt_boot

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
        # we change bootsector so the sysem could boot from it
        #TODO: move to specialized (NTFS) transfer target
        bootsector = DataExtent.DataExtent(0,512)
        extentswritten = 0
       
        for extent in volExtents:
            logging.debug("Transfering " + str(extent) + "...");
            #special handling for a boot options
            extentswritten = extentswritten + 1
            if ( extentswritten  % 100 == 0):
                #TODO: make better data approximation
                logging.info("% " + str(extentswritten) + " of " + str(len(volExtents)) + " original disk data have been transferred to the image ("+ str(extentswritten*100/len(volExtents)) +"%)" )
          
            if self.__fixNtBoot:
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