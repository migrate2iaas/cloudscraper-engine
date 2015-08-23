# --------------------------------------------------------
__author__ = "Alexey Kondratiev"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------
import ImageMediaFactory
import ImageMedia
import VhdQcow2Media

class VhdQcow2MediaFactory(ImageMediaFactory.ImageMediaFactory):
    """Abstract factory to create images"""
    
    def __init__(self, media_factory , qemu_path , dest_imagetype = "qcow2"):
        self.__media_factory = media_factory
        self.__qemu_path = qemu_path
        self.__dest_imagetype = dest_imagetype
        super(VhdQcow2MediaFactory , self).__init__()
        
        
    def createMedia(self , imagepath , imagesize):
        return VhdQcow2Media.VhdQcow2Media(
            self.__media_factory.createMedia(imagepath , imagesize) ,
            self.__qemu_path ,
            self.__dest_imagetype)

