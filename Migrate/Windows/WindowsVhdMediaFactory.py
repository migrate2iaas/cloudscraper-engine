# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------
import ImageMediaFactory
import Windows
import WindowsVhdMedia

class WindowsVhdMediaFactory(ImageMediaFactory.ImageMediaFactory):
    """Abstract factory to create images"""
    
    def __init__(self):
        self.__windows = Windows.Windows()
        super(WindowsVhdMediaFactory,self).__init__() 
        return

    def createMedia(self , imagepath , imagesize):
        media = None
        if self.__windows.getVersion() >= self.__windows.getSystemInfo().Win2008R2:
            media = WindowsVhdMedia.WindowsVhdMedia(imagepath, imagesize)
        else:
            logging.error("!!!ERROR: VHD images are supported on Windows 2008 R2 Server and higher systems only") 
        return media