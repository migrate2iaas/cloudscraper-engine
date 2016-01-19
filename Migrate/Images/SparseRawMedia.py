# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

from ImageMedia import ImageMedia
import os

import inspect


print("Imported sparse raw media")

class SparseRawMedia(ImageMedia):
    """Media representing one big but sparsed raw file"""

    def __init__(self, filename, size):
        """constructor"""
        super(SparseRawMedia,self).__init__() 
        self.__filename = filename
        self.__size = size
        
        

    def open(self):
        """
            Opens image files, prepares to write.
            Does nothing if already opened
            retuns None, throws and error if any   
        """
        file = open(self.__filename, 'wb')
        file.truncate(self.__size)

    def getMaxSize(self):
        """
            Returns the size (in bytes) of virtual disk represented by the image.
        """
        # NOTE: it may be truncated outside so we should update the max size too
        return max(self.__size, self.getImageSize())

    def getImageSize(self):
        """
            Gets the overall virtual container file size. 
            Note: it is subject to change cause the image file is used by other modules
        """
        return os.stat(self.__filename).st_size

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

    def allowDirectFileAccess(self):
        """
            Returns if the image file can be accessed as raw image from elsewhere
        """
        return True


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
        file = open(self.__filename , "rb")
        file.seek(offset)
        data = file.read(size)
        file.close()
        return data

   
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
        file = open(self.__filename , "r+b")
        file.seek(offset)
        file.write(data)
        file.close()


    def getFilePath(self):
        """
            Returns the file path associated with the image or None if there is no associated local path
        """
        return self.__filename
    
    def readDiskData(self , offset , size):
        """
        Reads data from the container (as it was a disk).

        Args:
            offset:long - offset from the virtual disk start in bytes
            data:str -  binary array of data to write. len(data) should be an integer of 512

        StreamVmdkMedia: IS NOT IMPLEMENTED, throw sNotImplementedError

        """
        return self.readImageData(offset, size)

    
