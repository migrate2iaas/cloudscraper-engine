# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

class DataTransferProto(object):
    """Data transfer protocol is abstraction of how the data is passed to the underlying media"""

    def writeData(self , extents):
        raise NotImplementedError

    def readData(self , extent):
        raise NotImplementedError

    def writeMetadata(self, extents):
        raise NotImplementedError

    def readMetadata(self, extent):
        raise NotImplementedError

    def getMedia(self):
        raise NotImplementedError

    #returns the overall size of underlying media
    def getSize(self):
        raise NotImplementedError


