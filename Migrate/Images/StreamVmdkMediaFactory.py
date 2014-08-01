# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------
import ImageMediaFactory
import StreamVmdkMedia

class StreamVmdkMediaFactory(ImageMediaFactory.ImageMediaFactory):
    """Abstract factory to create images"""
    
    def __init__(self):
        super(StreamVmdkMediaFactory,self).__init__() 
        
    def createMedia(self , imagepath , imagesize):
        media = StreamVmdkMedia.StreamVmdkMedia(imagepath , imagesize)
        return media



