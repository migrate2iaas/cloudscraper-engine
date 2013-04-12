# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import subprocess
import re
import os
import sys

import logging
import ImageMedia

# TODO: make media base class
class WindowsVhdMedia(ImageMedia.ImageMedia):
    """VHD disk created and managed by Win2008R2+ systems"""

    #it should generate RW-access (protocol to write data) so it could be accessed from elsewhere


    # 
    def __init__(self, filename, maxInBytes):
        # TODO: it's better to use CreateVirtualDisk() from WinAPI to acomplish the task
        # this one is created with diskpart
        logging.info("Initing new VHD disk " + filename + " of size " + str(maxInBytes) + " bytes");
        self.__fileName = filename
        sizemb = int(maxInBytes/1024/1024)
        if sizemb % (1024*1024):
            sizemb = sizemb + 1
        
        self.__diskNo = diskno = None
        self.__maxSizeMb = sizemb

        return 

    #starts the connection
    def open(self):
        if os.path.exists(self.__fileName):
            logging.error("!!!ERROR: image file" + self.__fileName + " already exists. Please, specify another one");
            raise IOError

        logging.debug("Initing new VHD disk");
        scriptpath = "diskpart_open.tmp"
        scrfile = open(scriptpath, "w+")
        script = "create vdisk file=\""+self.__fileName+"\" maximum="+str(self.__maxSizeMb) + " type=EXPANDABLE";
        script = script.__add__("\nattach vdisk");
        script = script.__add__("\nconvert mbr");
        script = script.__add__("\ndetail vdisk");
        scrfile.write(script);
        scrfile.close()

        logging.debug("Executing diskpart open script %s" , script);
        try:
            output = subprocess.check_output("diskpart /s \"" + scriptpath +"\"" , shell=True);
        except subprocess.CalledProcessError as ex:
            logging.error("!!!ERROR: Cannot create new VHD disk. Try to specify another disk image folder path")
            logging.error("Diskpart failed" + ex.output)
            logging.error("Script: \n" + script);
            raise

        match = re.search('Associated disk[#][:] ([0-9]+)',output)
        if match == None:
            logging.error("!!!ERROR: Cannot create new VHD disk.");
            logging.error("Diskpart bad output, cannot find disk associated. Output: %s", output)
            raise EnvironmentError
        diskno = int(match.group(1))
        self.__diskNo = diskno

        return True

    def getMaxSize(self):
        return self.__maxSize

    def reopen(self):
        self.close()
        self.open()
        return

    def close(self):
        scriptpath = "diskpart_close.tmp"
        scrfile = open(scriptpath, "w+")
        script = "select vdisk file=\""+self.__fileName+"\"";
        script = script + "\ndetach vdisk";
        scrfile.write(script);
        scrfile.close()

        logging.debug("Executing diskpart close script %s" , script);

        try:
            output = subprocess.check_output("diskpart /s \"" + scriptpath +"\"" , shell=True);
        except subprocess.CalledProcessError as ex:
            logging.error("!!!ERROR: Cannot release VHD image disk in order to upload it. Try to detach it manually and restart program with upload-only option.");
            logging.error("Diskpart failed" + ex.output)
            logging.error("Script: \n" + script);
            raise
        return True

    def flush(self):
        return
    
    def release(self):
        self.close()

    #override for WindowsMedia
    # returns path good for opening windows devices
    def getWindowsDevicePath(self):
        return "\\\\.\\PhysicalDrive" + str(self.__diskNo)

    def getWindowsDiskNumber(self):
        return self.__diskNo

    #sets the channel so the data may be sent whenever data changes
    def setChannel(self):
        return