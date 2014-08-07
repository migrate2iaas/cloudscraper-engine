# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------
import ImageMediaFactory
import StreamVmdkMedia
import math

class StreamVmdkMediaFactory(ImageMediaFactory.ImageMediaFactory):
    """Abstract factory to create images"""
    
    def __init__(self , compression = 4):
        super(StreamVmdkMediaFactory,self).__init__() 
        self.__compression = compression
        
    def createMedia(self , imagepath , imagesize):
        if imagesize:
            if not(imagesize % StreamVmdkMedia.GRAIN_SIZE == 0):
                imagesize = (int(imagesize / StreamVmdkMedia.GRAIN_SIZE) + 1) * StreamVmdkMedia.GRAIN_SIZE
        media = StreamVmdkMedia.StreamVmdkMedia(imagepath , imagesize, compression = self.__compression)
        return media



