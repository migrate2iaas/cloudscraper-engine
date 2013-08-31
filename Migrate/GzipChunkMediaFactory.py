# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------
import ImageMediaFactory
import GzipChunkMedia

class GzipChunkMediaFactory(ImageMediaFactory.ImageMediaFactory):
    """Abstract factory to create images"""
    
    def __init__(self, chunksize = 4096*1024, compression = 3):
        super(GzipChunkMediaFactory,self).__init__() 
        self.__compression = compression
        self.__chunkSize = chunksize

    def createMedia(self , imagepath , imagesize):
        media = GzipChunkMedia.GzipChunkMedia(imagepath , imagesize , self.__chunkSize , self.__compression)
        return media



