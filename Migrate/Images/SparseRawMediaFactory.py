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
        pass
        
    def createMedia(self , imagepath , imagesize):

        return SparseRawMedia.SparseRawMedia(imagepath, imagesize)

