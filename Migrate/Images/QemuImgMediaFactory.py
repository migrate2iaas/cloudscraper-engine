# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------
import ImageMediaFactory
import QemuImgMedia
import math

class QemuImgMediaFactory(ImageMediaFactory.ImageMediaFactory):
    """Abstract factory to create images"""
    
    def __init__(self):

        super(QemuImgMediaFactory,self).__init__()        

        
    def createMedia(self , imagepath , imagesize):
        return QemuImgMedia.QemuImgMedia(imagepath, imagesize)

