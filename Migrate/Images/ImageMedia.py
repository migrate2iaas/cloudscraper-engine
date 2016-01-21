# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

# the base class for all the media (VHD,raw files etc) to contain a system or data image
class ImageMedia(object):
    """Base class to represent media to store an image"""

    def open(self):
        """
            Opens image files, prepares to write.
            Does nothing if already opened
            retuns None, throws and error if any   
        """
        return None

    def getMaxSize(self):
        """
            Returns the size (in bytes) of virtual disk represented by the image.
        """
        return 0

    #gets the overall image size available for writing. Note: it is subject to grow when new data is written
    def getImageSize(self):
        """
            Gets the overall virtual container file size. 
            Note: it is subject to grow when new data is written
        """
        raise NotImplementedError

    def reopen(self):
        """
            Reopens image files
            retuns None, throws and error if any  
        """
        self.close()
        self.open()

    def close(self):
        """
            Closes the image files and finalizing images. No data could be written after close()
            retuns None, throws and error if any  
        """
        return

    def flush(self):
        """
            (Optional) Flushes all intermediate data to the disk
        """
        return
    
    def release(self):
        """
            No special handing for now. Just call close()
        """
        self.close()


    #reads data from image, returns data buffer
    def readImageData(self , offset , size):
        """
        Reads data from image file.

        Args:
            offset:long - offset from the file start in bytes
            size:int - the size of buffer to read

        Returns str() or bytearray() buffer with binary data read
        Throws an error if any
        """
        raise NotImplementedError

   
    def writeDiskData(self, offset, data):
        """
        Write data to the virtual disk.
        
        Args:
            offset:long - offset from the virtual disk start in bytes
            data:str -  binary array of data to write. len(data) should be an integer of 512
        
        Note: Data is written sequentially but there could be gaps that should be treated as null-blocks. 
        E.g. the first call writes to interval [0;4096) but the second writes to [8192;32537). The interval [4096;8192) should be filled with nulls.

        Returns None, throws an error if any
        """
        raise NotImplementedError

    def getFilePath(self):
        """
            Returns the file path associated with the image or None if there is no associated local path
        """
        raise NotImplementedError
    
    def readDiskData(self , offset , size):
        """
        Reads data from the container (as it was a disk).

        Args:
            offset:long - offset from the virtual disk start in bytes
            data:str -  binary array of data to write. len(data) should be an integer of 512

        StreamVmdkMedia: IS NOT IMPLEMENTED, throw sNotImplementedError

        """
        raise NotImplementedError

    #sets the channel so the data may be sent simultaniously. Not implemented for now
    def setChannel(self):
        raise NotImplementedError