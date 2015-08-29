# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------
import ImageMediaFactory
import SparseRawMedia
import math

class SparseRawMediaFactory(ImageMediaFactory.ImageMediaFactory):
    """Abstract factory to create images"""
    
    def __init__(self):
        super(SparseRawMediaFactory,self).__init__()
        
        
    def createMedia(self , imagepath , imagesize):
        media = SparseRawMedia.SparseRawMedia(imagepath, imagesize)
        return media

