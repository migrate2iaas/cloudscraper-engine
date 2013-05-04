# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import DataTransferProto
import ImageMedia
import DataExtent

class SimpleDataTransferProto(DataTransferProto.DataTransferProto):
    """Data transfer protocol to just pass the data to the underlying media"""

    def __init__(self , media):
        self.__media = media

    def writeData(self , extents):
        for extent in extents:
            self.__media.writeDiskData(extent.getStart() , extent.getData())

    def readData(self , extent):
        return self.__media.readDiskData(extent.getStart() , extent.getSize())

    def writeMetadata(self, extents):
        return self.writeData(extents)

    def readMetadata(self, extent):
        return self.readData(extent)

    def getMedia(self):
        return self.__media

    #returns the overall size of underlying media
    def getSize(self):
        return self.__media.getMaxSize()


