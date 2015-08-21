# --------------------------------------------------------
__author__ = "Alexey Kondratiev"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------
import ImageMediaFactory
import VhdQcow2Media

class VhdQcow2MediaFactory(ImageMediaFactory.ImageMediaFactory):
    """Abstract factory to create images"""
    
    def __init__(self, qemu_path):
        self.__qemu_path = qemu_path
        super(VhdQcow2MediaFactory,self).__init__()
        
        
    def createMedia(self , imagepath , imagesize):
        return VhdQcow2Media.VhdQcow2Media(self.__qemu_path, imagepath, imagesize)

