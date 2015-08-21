# --------------------------------------------------------
__author__ = "Alexey Kondratiev"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import ImageMedia
import WindowsVhdMedia
import os
import subprocess
import logging

# the base class for all the media (VHD,raw files etc) to contain a system or data image
class VhdQcow2Media(ImageMedia.ImageMedia):

    def __init__(self , qemu_path , filename , max_in_bytes , fixed = False , align_disk = 0):
        """constructor"""
        self.__qemu_path = qemu_path
        self.__filename = filename + ".vhd"
        self.__vhdmedia = WindowsVhdMedia.WindowsVhdMedia(self.__filename , max_in_bytes , fixed , align_disk);
        super(VhdQcow2Media , self).__init__() 

    def open(self):
        self.__vhdmedia.open();

    def getMaxSize(self):
        """
            Returns the size (in bytes) of virtual disk represented by the image.
        """
        return self.__vhdmedia.getMaxSize()

    def getImageSize(self):
        """
            Gets the overall virtual container file size. 
            Note: it is subject to change cause the image file is used by other modules
        """
        return os.stat(self.getFilePath()).st_size


    def reopen(self):
        self.__vhdmedia.reopen()


    def close(self):
        """
            Closes the image files and finalizing images. No data could be written after close()
            retuns None, throws and error if any  
        """
        if not hasattr(self.close.__func__, "is_closed"):
            self.close.__func__.is_closed = False
        else:
            return True
        
        self.__vhdmedia.close()
        try:
            output = subprocess.check_output(
                "\"" + self.__qemu_path + "\\qemu-img\" convert -O qcow2 " +
                "\"" + self.getFilePath() + "\" " + self. getFilePath().replace(".vhd" , "" , 1) ,
                shell = True);
        except subprocess.CalledProcessError as ex:
            logging.error("!!!ERROR: Cannot convert .vhd image type to .qcow2")
            logging.error("qemu-img failed" + ex.output)
            raise

        return True


    def flush(self):
        """
            (Optional) Flushes all intermediate data to the disk
        """
        return self.__vhdmedia.flush()
    

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
        return self.__vhdmedia.readImageData(offset , size)

   
    def writeDiskData(self , offset , data):
        """
        Write data to the virtual disk.
        
        Args:
            offset:long - offset from the virtual disk start in bytes
            data:str -  binary array of data to write. len(data) should be an integer of 512
        
        Returns None, throws an error if any
        """
        self.__vhdmedia.writeDiskData(offset, data)


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
        return self.__vhdmedia.readDiskData(offset, size)

    #sets the channel so the data may be sent simultaniously. Not implemented for now
    def setChannel(self):
        raise NotImplementedError