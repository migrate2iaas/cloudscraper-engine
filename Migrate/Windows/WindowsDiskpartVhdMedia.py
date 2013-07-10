# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import subprocess
import re
import os
import sys

sys.path.append('.\..')
sys.path.append('.\..\Windows')
sys.path.append('.\Windows')


import win32file
import win32event
import win32con
import win32security
import win32api
import pywintypes
import ctypes
import winioctlcon
import struct
import ntsecuritycon
import VolumeInfo
import os
import logging
import ImageMedia

from MigrateExceptions import *

# TODO: make media base class
class WindowsDiskpartVhdMedia(ImageMedia.ImageMedia):
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
        self.__hDrive = None

        return 

    #internal function to open drive
    def opendrive(self):
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        drivename = self.getWindowsDevicePath()
        logging.debug("Openning disk %s" , drivename);
        filename = drivename
        try:
            self.__hDrive = win32file.CreateFile( drivename, win32con.GENERIC_READ | win32con.GENERIC_WRITE| ntsecuritycon.FILE_READ_ATTRIBUTES , win32con. FILE_SHARE_READ | win32con. FILE_SHARE_WRITE, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        except Exception as ex:
            raise FileException(filename , ex)

    def closedrive(self):
        win32file.CloseHandle(self.__hDrive)

    #starts the connection
    def open(self):
        if os.path.exists(self.__fileName):
            #NOTE in this case only readImageData is supported
            logging.warning("!Image file" + self.__fileName + " opened to read file data only.");
            return True
            
            #raise IOError("image file" + self.__fileName + " already exists. Please, specify another one")

        logging.debug("Initing new VHD disk");
        scriptpath = "diskpart_open.tmp"
        scrfile = open(scriptpath, "w+")
        script = "create vdisk file=\""+self.__fileName+"\" maximum="+str(self.__maxSizeMb) + " type=EXPANDABLE";
        script = script.__add__("\nattach vdisk");
        script = script.__add__("\nconvert mbr");
        script = script.__add__("\ndetail vdisk");
        scrfile.write(script);
        scrfile.close()

        windir = os.environ['windir']

        proc_import_sys = subprocess.Popen([windir+'\\system32\\cmd.exe', '/C', 'diskpart', '/s' , scriptpath ]
                  , bufsize=1024*1024*128, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        logging.debug("Executing diskpart open script %s" , script);
        (stdoutdata, stderrdata) = proc_import_sys.communicate();
        if proc_import_sys.returncode != 0 or stderrdata:
            logging.error("!!!ERROR: Cannot create new VHD disk. Try to specify another folder for disk image")
            logging.error("Diskpart failed \n error: " + stderrdata )
            logging.error("Diskpart failed \n out: " + stdoutdata )
            logging.error("Script:" + script);
            return False
        
        output = stdoutdata;    

        match = re.search('Associated disk[#][:] ([0-9]+)',output)
        if match == None:
            logging.error("!!!ERROR: Cannot create new VHD disk.");
            logging.error("Diskpart bad output, cannot find disk associated. Output: %s", output)
            raise EnvironmentError("Bad diskpart output")
        diskno = int(match.group(1))
        self.__diskNo = diskno
        self.opendrive()

        return True

    def getMaxSize(self):
        return self.__maxSizeMb*1024*1024

    def reopen(self):
        self.close()
        self.open()
        return

    def close(self):
        self.closedrive()

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


     #reads data from image, returns data buffer
    def readImageData(self , offset , size):
        diskfile = open(self.__fileName, "rb")
        diskfile.seek(offset)
        data = diskfile.read(size)
        diskfile.close()
        return data

    #writes data to the container (as it was a disk)
    def writeDiskData(self, offset, data):
        if self.__hDrive == None:
            raise IOError("Use open first to create vhd disk media")
        filename = self.getWindowsDevicePath()
        try:
            win32file.SetFilePointer(self.__hDrive, offset, win32con.FILE_BEGIN)
            (result , output) = win32file.WriteFile(self.__hDrive,data)
        except Exception as ex:
            raise FileException(filename , ex)
        return output

    #reads data from the container (as it was a disk)
    def readDiskData(self , offset , size):
        if self.__hDrive == None:
            raise IOError("Use open first to create vhd disk media")
        filename = self.getWindowsDevicePath()
        try:
            win32file.SetFilePointer(self.__hDrive, offset, win32con.FILE_BEGIN)
            (result , output) = win32file.ReadFile(self.__hDrive,size,None)
        except Exception as ex:
            raise FileException(filename , ex)
        return output
       
        

    #gets the overall image size available for writing. Note: it is subject to grow when new data is written
    def getImageSize(self):
        return os.stat(self.__fileName).st_size

    #override for WindowsMedia
    # returns path good for opening windows devices
    def getWindowsDevicePath(self):
        return "\\\\.\\PhysicalDrive" + str(self.__diskNo)

    def getWindowsDiskNumber(self):
        return self.__diskNo

    #sets the channel so the data may be sent whenever data changes
    def setChannel(self):
        return