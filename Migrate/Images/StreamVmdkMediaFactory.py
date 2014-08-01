# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------
import ImageMediaFactory
import StreamVmdkMediaFactory

class StreamVmdkMediaFactory(ImageMediaFactory.ImageMediaFactory):
    """Abstract factory to create images"""
    
    def __init__(self):
        super(StreamVmdkMediaFactory,self).__init__() 
        self.__compression = compression
        self.__chunkSize = chunksize

    def createMedia(self , imagepath , imagesize):
        media = StreamVmdkMedia.StreamVmdkMedia(imagepath , imagesize)
        return media



