# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import TransferTarget
import FileToBackup
import DataExtent
import WindowsVolume
from MigrateExceptions import FileException

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
import logging

class WindowsVolumeTransferTarget(TransferTarget.TransferTarget):
    """Transfer target represented as Windows (NTFS) volume"""

    def __init__(self, volumeName, media = None):    
        self.__volumeName = volumeName.lower()
        # we save the underlying media thus we may free it when we stopped using it
        self.__media = media
    
    # writes the file
    # what to do with metadata and stuff?
    def transferFile(self, fileToBackup):
         return

    # transfers file data only, no metadata should be written
    def transferFileData(self, fileName, fileExtentsData):
        #TODO: think of metadata and filetypes. 
        # BackupRead and BackupWrite could be used. 
        # But only data backed by BackupRead could be used for BackupWrite

        # The SE_BACKUP_NAME and SE_RESTORE_NAME access privileges were specifically created to provide this ability to backup applications. 
        logging.debug("Open file %s" , self.__volumeName + "\\" + fileName)
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()

        filename = self.__volumeName + "\\" + fileName
        try:
            hfile = win32file.CreateFile( self.__volumeName + "\\" + fileName, win32con.GENERIC_READ | win32con.GENERIC_WRITE | ntsecuritycon.FILE_READ_ATTRIBUTES | ntsecuritycon.FILE_WRITE_ATTRIBUTES, win32con. FILE_SHARE_READ|win32con.FILE_SHARE_WRITE, secur_att,   win32con.OPEN_ALWAYS, win32con.FILE_ATTRIBUTE_NORMAL | win32con.FILE_FLAG_BACKUP_SEMANTICS , 0 )
            for volextent in volumeDataExtents:
                win32file.SetFilePointer(hfile, volextent.getStart(), win32con.FILE_BEGIN)
                win32file.WriteFile(hfile,extent.getData(),None)
            win32file.CloseHandle(hfile)
        except Exception as ex:
            raise FileException(filename , ex)

        return
        

    # transfers file data only
    def transferRawData(self, volumeDataExtents):
        # open as raw
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        logging.debug("Open file %s" , self.__volumeName)
        filename = self.__volumeName 
        try:
            hfile = win32file.CreateFile( self.__volumeName, win32con.GENERIC_READ | win32con.GENERIC_WRITE | ntsecuritycon.FILE_READ_ATTRIBUTES | ntsecuritycon.FILE_WRITE_ATTRIBUTES, win32con. FILE_SHARE_READ|win32con.FILE_SHARE_WRITE, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
            # lock it
            outbuffer = win32file.DeviceIoControl(hfile,  winioctlcon.FSCTL_LOCK_VOLUME,  None, None, None )
        except Exception as ex:
            raise FileException(filename , ex)
        # writing data
        # note: maybe to use multithreading in here, dunno


        # NOTE: we substract the $boot file from the metadata here
        # later, some better adjusts should be found too...
        bootfile = DataExtent.DataExtent(0,4096)
        extentswritten = 0

        logging.info("Data transfer to the virtual disk image has started!")
        
        try:
            for volextent in volumeDataExtents:
                #special handling for a boot options
                if bootfile in volextent:
                    logging.debug("Skipping $boot extent in " + str(volextent) )
                    for bootextent in volextent.substract(bootfile):
                        logging.debug("Write boot extent "+ str(bootextent) )
                        win32file.SetFilePointer(hfile, bootextent.getStart(), win32con.FILE_BEGIN)
                        win32file.WriteFile(hfile,bootextent.getData(),None)
                    continue

                logging.debug("Write extent " + str(volextent) )
                win32file.SetFilePointer(hfile, volextent.getStart(), win32con.FILE_BEGIN)
                win32file.WriteFile(hfile,volextent.getData(),None)
                extentswritten = extentswritten + 1
                if ( extentswritten  % 100 == 0):
                    logging.info("% " + str(extentswritten) + " of " + str(len(volumeDataExtents)) + " original disk extents have been transferred to the image ("+ str(extentswritten*100/len(volumeDataExtents)) +"%)" )
          
            win32file.CloseHandle(hfile)
            logging.info("%  Disk image has been successfully created (100%)" )
        except Exception as ex:
            raise FileException(filename , ex)

    # transfers raw metadata, it should be precached 'cause it's up to change afterwards
    def transferRawMetadata(self, volumeDataExtents):
        # the same for now
        return self.transferRawData(volumeDataExtents)

    def deleteFileTransfer(self , fileName):
        filename = self.__volumeName+"\\"+fileName
        try:
            win32file.DeleteFile(filename)
        except Exception as ex:
            raise FileException(filename , ex)

    def close(self):
        # releases underlying media after the operation is done
        if self.__media:
            self.__media.release()

    def cancelTransfer(self):
        #to get parser and discard me
        return