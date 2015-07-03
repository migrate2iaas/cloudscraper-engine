# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import ImageMedia
import os
import logging
from subprocess import *

class QemuImgMedia(ImageMedia.ImageMedia):
    """Media created by qemu-img utility"""

    def __init__(self, filename, size):
        """constructor"""
        super(QemuImgMedia,self).__init__() 
        self.__filename = filename
        self.__size = size
        self.__compression = False
        
        format = "raw"
        if str(filename).lower().endswith("vhd"):
            format = "vpc"
        if str(filename).lower().endswith("vmdk"):
            format = "vmdk"
        if str(filename).lower().endswith("qcow"):
            format = "qcow"
        if str(filename).lower().endswith("qcow2"):
            format = "qcow2"
        self.__format = format

        if ".img" in filename or ".qcow" in filename or ".qcow2" in filename:
            self.__compression = True
        
        self.__options = []

        if self.__compression == True:
            self.__options.append("-c")

        if ".fixed" in filename:
            self.__options.append("subformat=fixed")


    def open(self):
        """
            Opens image files, prepares to write.
            Does nothing if already opened
            retuns None, throws and error if any   
        """
        #TODO: should try to check various formats
        #TODO: consider using snapshots as it may create good delta-backup opportunity
        #NOTE: subformat can also be specified
        if len(self.__options):
            qemu_cmd = ["qemu-img" , "create" , "-f" , self.__format  , "-o"] + self.__options + [self.__filename , str(self.__size)]
        else:
            qemu_cmd = ["qemu-img" , "create" , "-f" , self.__format , self.__filename , str(self.__size)]

        p = Popen(qemu_cmd, stdout=PIPE , stderr=PIPE)
        cmd_output = p.communicate()

        if p.returncode:
            logging.error('!!!ERROR: Error while running qemu-img return_code = %s\n'
              'stdout=%s\nstderr=%s',
            p.returncode, cmd_output[0], cmd_output[1])
            raise IOError("qemu-img failed")

        logging.debug ("qemu-img created the image " + str(cmd_output[0]))
                        
            
        
   

    def getMaxSize(self):
        """
            Returns the size (in bytes) of virtual disk represented by the image.
        """
        return self.__size

    def getImageSize(self):
        """
            Gets the overall virtual container file size. 
            Note: it is subject to change cause the image file is used by other modules
        """
        return os.stat(self.__filename).st_size

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
        file = open(self.__filename , "rb")
        file.seek(offset)
        data = file.read(size)
        file.close()
        return data

   
    def writeDiskData(self, offset, data):
        """
        Write data to the virtual disk.
        !Not implemented.
        
        
        Args:
            offset:long - offset from the virtual disk start in bytes
            data:str -  binary array of data to write. len(data) should be an integer of 512
        
        Note: Data is written sequentially but there could be gaps that should be treated as null-blocks. 
        E.g. the first call writes to interval [0;4096) but the second writes to [8192;32537). The interval [4096;8192) should be filled with nulls.

        Returns None, throws an error if any
        """
        #TODO: we can utilize qemu-ndb here
        raise NotImplementedError


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
        raise NotImplementedError

    #sets the channel so the data may be sent simultaniously. Not implemented for now
    def setChannel(self):
        raise NotImplementedError