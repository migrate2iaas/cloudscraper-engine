# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------



class ImageMediaFactory(object):
    """Abstract factory to create images"""

    def __init__(self):
        return

    def createMedia(self , imagepath , imagesize):
        raise NotImplementedError
