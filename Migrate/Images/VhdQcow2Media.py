# --------------------------------------------------------
__author__ = "Alexey Kondratiev"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import ImageMedia
import os
import subprocess
import logging

# the base class for all the media (VHD,raw files etc) to contain a system or data image
class VhdQcow2Media(ImageMedia.ImageMedia):

    def __init__(self , image_media , qemu_path , dest_imagetype, qemu_convert_params = ""):
        """constructor"""
        self.__qemu_path = qemu_path
        self.__dest_imagetype = dest_imagetype
        self.__is_closed = False
        self.__media = image_media
        self.__qemu_convert_params = qemu_convert_params
        self.__filename = self.__media.getFilePath() + "." +  self.__dest_imagetype
        super(VhdQcow2Media , self).__init__() 

    def open(self):
        self.__media.open();

    def getMaxSize(self):
        """
            Returns the size (in bytes) of virtual disk represented by the image.
        """
        return self.__media.getMaxSize()

    def getImageSize(self):
        """
            Gets the overall virtual container file size. 
            Note: it is subject to change cause the image file is used by other modules
        """
        return self.__media.getImageSize()


    def reopen(self):
        self.__media.reopen()


    def close(self):
        """
            Closes the image files and finalizing images. No data could be written after close()
            retuns None, throws and error if any  
        """
        if (self.__is_closed == False):
            self.__is_closed = True
        else:
            return True
        
        self.__media.close()

        #Getting image extention, then converting current image type to dest_imagetype
        try:
            output = subprocess.check_output(
                "\"" + self.__qemu_path + "\\qemu-img\" convert -O " +  self.__dest_imagetype + self.__qemu_convert_params + " \""  + 
                self.__media.getFilePath() + "\" " + self.getFilePath() + "\"" ,
                shell = True);
        except subprocess.CalledProcessError as ex:
            logging.error("!!!ERROR: Cannot convert image type to .qcow2")
            logging.error("qemu-img failed" + ex.output)
            raise

        return True


    def flush(self):
        """
            (Optional) Flushes all intermediate data to the disk
        """
        return self.__media.flush()
    

    def release(self):
        """
            No special handing for now. Just call close()
        """
        return self.close()


    #reads data from image, returns data buffer
    def readImageData(self , offset , size):
        """
            No special handing for now.
        """
        return self.__media.readImageData(offset , size)

   
    def writeDiskData(self , offset , data):
        """
        Write data to the virtual disk.
        
        Args:
            offset:long - offset from the virtual disk start in bytes
            data:str -  binary array of data to write. len(data) should be an integer of 512
        
        Returns None, throws an error if any
        """
        self.__media.writeDiskData(offset, data)


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
        return self.__media.readDiskData(offset, size)

    #sets the channel so the data may be sent simultaniously. Not implemented for now
    def setChannel(self):
        raise NotImplementedError