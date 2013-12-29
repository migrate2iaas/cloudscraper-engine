# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------
import ImageMediaFactory
import Windows
import WindowsVhdMedia
import logging
import os
import time

class WindowsVhdMediaFactory(ImageMediaFactory.ImageMediaFactory):
    """Abstract factory to create images"""
    
    def __init__(self , allow_recreate = False, fixed = False):
        self.__windows = Windows.Windows()
        self.__allowRecreate = allow_recreate
        self.__fixed = fixed
        super(WindowsVhdMediaFactory, self).__init__() 
        return

    def createMedia(self , imagepath , imagesize):
        """
        Creates VHD media
        Args:

            imagepath: str - local path to store VHD
            imagesize: long - size of image in bytes
            fixed: Bool - if VHD is fixed (preallocated) or dynamic
        """
        media = None

        #if self.__allowRecreate == False:
        #    if os.path.exists(imagepath):
        #        #rename existing image
        #        renameto = imagepath+"."+str( int(time.time()) )
        #        os.rename(imagepath , renameto )
        #        logging.warning("!Warning: there is already an image file " + str(imagepath) + " present. Renaming it to " +  str(renameto) + " preserve your data");
                

        if self.__windows.getVersion() >= self.__windows.getSystemInfo().Win2008R2:
            media = WindowsVhdMedia.WindowsVhdMedia(imagepath, imagesize, self.__fixed)
        else:
            logging.error("!!!ERROR: VHD images are supported on Windows 2008 R2 Server and higher systems only") 

        return media