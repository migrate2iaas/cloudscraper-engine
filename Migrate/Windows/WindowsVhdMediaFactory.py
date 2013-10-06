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
    
    def __init__(self , allow_recreate = False):
        self.__windows = Windows.Windows()
        self.__allowRecreate = allow_recreate
        super(WindowsVhdMediaFactory, self).__init__() 
        return

    def createMedia(self , imagepath , imagesize):
        media = None

        #if self.__allowRecreate == False:
        #    if os.path.exists(imagepath):
        #        #rename existing image
        #        renameto = imagepath+"."+str( int(time.time()) )
        #        os.rename(imagepath , renameto )
        #        logging.warning("!Warning: there is already an image file " + str(imagepath) + " present. Renaming it to " +  str(renameto) + " preserve your data");
                

        if self.__windows.getVersion() >= self.__windows.getSystemInfo().Win2008R2:
            media = WindowsVhdMedia.WindowsVhdMedia(imagepath, imagesize)
        else:
            logging.error("!!!ERROR: VHD images are supported on Windows 2008 R2 Server and higher systems only") 

        return media