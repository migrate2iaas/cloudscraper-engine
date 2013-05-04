# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

# the base class for all the media (VHD,raw files etc) to contain a system or data image
class ImageMedia(object):
    """Base class to represent media to store an image"""

    #starts the connection
    def open(self):
        raise NotImplementedError

    def getMaxSize(self):
        raise NotImplementedError

    def reopen(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError
    
    def release(self):
        raise NotImplementedError

    #reads data from image, returns data buffer
    def readImageData(self , offset , size):
        raise NotImplementedError

    #writes data to the container (as it was a disk)
    def writeDiskData(self, offset, data):
        raise NotImplementedError

    #reads data from the container (as it was a disk)
    def readDiskData(self , offset , size):
        raise NotImplementedError


    #gets the overall image size available for writing. Note: it is subject to grow when new data is written
    def getImageSize(self):
        raise NotImplementedError

    #sets the channel so the data may be sent simultaniously. Not implemented for now
    def setChannel(self):
        raise NotImplementedError