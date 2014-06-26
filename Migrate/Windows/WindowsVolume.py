# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys
sys.path.append(sys.path[0]+'\\..')
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
from collections import namedtuple

from DataExtent import DataExtent

import logging

class DeferedReader(object):
    """Defers reading extent from the volume extent"""
    def __init__(self, volExtent, volume):
        self.__volExtent = volExtent
        self.__volume = volume
    def __str__(self):
        return self.__volume.readExtent(self.__volExtent)


class AllFilesIterator(object):
    """Iterator for looping over a sequence backwards."""

    def __init__(self, rootIterator, rootPath):
        self.__rootPath = rootPath
        self.__rootIterator = rootIterator
        self.__currentIterator = None
    
    def __iter__(self):
        return self

    #returns name of the file
    def next(self):
        
        try:
            if self.__currentIterator:
                return self.__currentIterator.next()
        except StopIteration:
            self.__currentIterator = None

        data = self.__rootIterator.next()
        while data[8] == "." or data[8] == ".." or data[8] == "System Volume Information":
            data = self.__rootIterator.next()

        # data[0] == attributes
        if (data[0] & win32con.FILE_ATTRIBUTE_DIRECTORY):
            currentpath = self.__rootPath+'\\'+data[8]
            try:
                self.__currentIterator = AllFilesIterator(win32file.FindFilesIterator(currentpath+'\\*', None ) , currentpath)
            except Exception as ex:
                raise FileException(currentpath , ex)
            return self.next()
        else:
            #data[8] == filename
            #check it already ends with \
            if self.__rootPath[-1] == '\\':
                return self.__rootPath+data[8]
            else:
                return self.__rootPath+'\\'+data[8]



#
#TODO: make backup source data base class
class WindowsVolume(object):
    """Windows volume"""
    
    def __init__(self, volumeName , max_reported_extent_size=16*1024*1024):
        """
        constructor

        Args:
        volumeName :str - path to the volume to be opened
        max_reported_extent_size: int, long - size of maximum extent size reported by any of funcitons that gets extents.
        If the extent is larger it is split on few smaller ones of size <= max_reported_extent_size

        """
       
        self.__maxReportedExtentSize = max_reported_extent_size

        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        
        self.__volumeName = volumeName.lower()

        logging.debug("Openning volume %s" , self.__volumeName) 
        filename = self.__volumeName
        try:
            self.__hfile = win32file.CreateFile( filename, win32con.GENERIC_READ|  win32con.GENERIC_WRITE | ntsecuritycon.FILE_READ_ATTRIBUTES | ntsecuritycon.FILE_WRITE_ATTRIBUTES, win32con. FILE_SHARE_READ|win32con.FILE_SHARE_WRITE, secur_att,   win32con.OPEN_ALWAYS, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        except Exception as ex:
            raise FileException(filename , ex)

        #TODO: impersonate with backup priveleges
        
        output_buffer_size = 256 
        try:
            outbuffer = win32file.DeviceIoControl(self.__hfile,  winioctlcon.FSCTL_GET_NTFS_VOLUME_DATA,  None, output_buffer_size, None )
        except Exception as ex:
            raise FileException(filename , ex)
       
        if outbuffer:
            self.__bytesPerCluster = struct.unpack("@qqqqqIIIIqqqqq",outbuffer[0:96])[6]
            logging.debug("Opened volume %s for reading, bytes per cluster %d" , self.__volumeName , self.__bytesPerCluster) 
            self.__filesystem = 'NTFS' 

        return 

    # gets the volume size
    def getVolumeSize(self):
            # typedef struct {
            #LARGE_INTEGER Length 
            #} GET_LENGTH_INFORMATION;
        IOCTL_DISK_GET_LENGTH_INFO = 0x7405c
        outbuffersize = 8
        
        filename = self.__volumeName
        try:
            outbuffer = win32file.DeviceIoControl(self.__hfile, IOCTL_DISK_GET_LENGTH_INFO , None , outbuffersize , None )
        except Exception as ex:
            raise FileException(filename , ex)

        return struct.unpack("@q" , outbuffer)[0]

    # gets the file enumerator (iterable). each of it's elements represent a filename (unicode string)
    # one may specify another mask
    def getFileEnumerator(self , rootpath = "\\" , mask = "*"):
        filename = self.__volumeName+rootpath
        try:
            return AllFilesIterator(win32file.FindFilesIterator(self.__volumeName + rootpath + mask, None ) , filename)
        except Exception as ex:
            raise FileException(filename , ex)

    def getVolumeName(self):
        return self.__volumeName
    
    # returns iterable of DataExtent structs with data filled
    def getFileDataBlocks(self, fileName):
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        logging.debug("Openning " + self.__volumeName+"\\"+fileName)

        filename = self.__volumeName+"\\"+fileName
        try:
            hFile = win32file.CreateFile(filename ,ntsecuritycon.FILE_READ_ATTRIBUTES, win32con.FILE_SHARE_READ + win32con.FILE_SHARE_WRITE, secur_att,   win32con.OPEN_EXISTING, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        except Exception as ex:
            raise FileException(filename , ex)
       
        # input
        # STARTING_VCN_INPUT_BUFFER
        #typedef struct {
        #LARGE_INTEGER StartingVcn;
        #} STARTING_VCN_INPUT_BUFFER, *PSTARTING_VCN_INPUT_BUFFER;
        #
        vcn_input_buffer = struct.pack('=q',0)

        #TODO: check on really big files
        vcn_ouput_buffer_size = 128*1024*1024

        #TODO: handle if buffer was too small
        try:
            outbuffer = win32file.DeviceIoControl(hFile,  winioctlcon.FSCTL_GET_RETRIEVAL_POINTERS,  vcn_input_buffer, vcn_ouput_buffer_size, None )
        except Exception as ex:
            raise FileException(filename , ex)

        #typedef struct RETRIEVAL_POINTERS_BUFFER {
        #DWORD         ExtentCount;
        #LARGE_INTEGER StartingVcn;
        #struct {
        #LARGE_INTEGER NextVcn;
        #LARGE_INTEGER Lcn;
        #} Extents[1];
        #} RETRIEVAL_POINTERS_BUFFER, *PRETRIEVAL_POINTERS_BUFFER;
        # @Iq = dword + large_integer

        retrieval_pointers_buffer_headertuple = namedtuple('RETRIEVAL_POINTERS_BUFFER', 'ExtentCount StartingVcn')
        retrieval_pointers_buffer_header = retrieval_pointers_buffer_headertuple._make(struct.unpack("@Iq" , outbuffer[0:16]))
        
        buffer_offset = struct.calcsize("@Iq")

        extent_count = 0
        current_file_vcn = retrieval_pointers_buffer_header.StartingVcn
        volextents = list() 
        extenttuple = namedtuple('Extents', 'NextVcn Lcn')
        while extent_count < retrieval_pointers_buffer_header.ExtentCount:
            
            extent = extenttuple._make(struct.unpack_from("@qq", outbuffer[buffer_offset:buffer_offset+struct.calcsize("@qq")]))
           
            volextent = DataExtent(extent.Lcn * self.__bytesPerCluster, (extent.NextVcn - current_file_vcn) * self.__bytesPerCluster )
            volextent.setData(DeferedReader(volextent, self) )
            volextents.append(volextent)
            logging.debug("Found extent " + str(volextent))

            current_file_vcn = extent.NextVcn
            buffer_offset = buffer_offset + struct.calcsize("@qq")
            extent_count = extent_count + 1

        return volextents

    #gets filesystem string 
    def getFileSystem(self):
        return self.__filesystem

    #returns iterable of filled bitmap extents with getData available
    def getFilledVolumeBitmap(self):
        
        # should be enough to hold a bitmap for 2 Tb disk, not sure, got some bugs on it
        bitmap_out_buffer_size = 128*1024*1024

        # input
        # STARTING_VCN_INPUT_BUFFER
        #typedef struct {
        #LARGE_INTEGER StartingVcn;
        #} STARTING_VCN_INPUT_BUFFER, *PSTARTING_VCN_INPUT_BUFFER;
        # should be 0 for the overall bitmap
        vcn_input_buffer = struct.pack('=q',0)
        logging.debug("Getting full volume bitmap")
        
        filename = self.__volumeName
        try:
            outbuffer = win32file.DeviceIoControl(self.__hfile,  winioctlcon.FSCTL_GET_VOLUME_BITMAP,  vcn_input_buffer, bitmap_out_buffer_size, None )
        except Exception as ex:
            raise FileException(filename , ex)

        
        #typedef struct {
        #LARGE_INTEGER StartingLcn;
        #LARGE_INTEGER BitmapSize;
        #BYTE          Buffer[1];
        #} VOLUME_BITMAP_BUFFER, *PVOLUME_BITMAP_BUFFER;
        volume_bitmap_buffer_headertuple = namedtuple('VOLUME_BITMAP_BUFFER', 'StartingLcn BitmapSize')
        volume_bitmap_buffer_header = volume_bitmap_buffer_headertuple._make(struct.unpack("@qq" , outbuffer[0:struct.calcsize("@qq")]))
        
        bits_left = volume_bitmap_buffer_header.BitmapSize 
        current_start = volume_bitmap_buffer_header.StartingLcn 
        buffer_offset = struct.calcsize("@qq")
        last_start = long(0) 
        last_size = long(0) 
        volextents = list()
        while bits_left > 0:
            byte = ord(outbuffer[buffer_offset])
            # our granularity is 8 clusters, mark all used even if one of 8 is used
            if byte > 0: 
                if last_size == 0:
                    last_start = current_start 
                last_size = last_size + 8 * self.__bytesPerCluster     
            else: 
                if last_size:
                    volextent = DataExtent(last_start, last_size)
                    volextent.setData(DeferedReader(volextent, self) )
                    volextents.append(volextent)
                last_size = 0 
                last_start = 0 
            
            #Here we decide what extent size is treated as max
            #TODO: move it somewhere else. Bad to see it here
            
            if last_size >= self.__maxReportedExtentSize:
                volextent = DataExtent(last_start, last_size)
                volextent.setData(DeferedReader(volextent, self) )
                volextents.append(volextent)
                last_size = 0 
                last_start = 0 


            bits_left = bits_left - 8 
            buffer_offset = buffer_offset + 1
            current_start += 8*self.__bytesPerCluster 
        
        if last_size:
            volextent = DataExtent(last_start, last_size)
            volextent.setData(DeferedReader(volextent, self) )
            volextents.append(volextent)
        

        return volextents

    #returns bytes read
    def readExtent(self, volextent):
        filename = self.__volumeName
        try:
            # we should read several blocks. Big chunks of data read could cause vss to fail
            size = volextent.getSize()
            win32file.SetFilePointer(self.__hfile, volextent.getStart(), win32con.FILE_BEGIN)
            output = bytearray("")
            while size > self.__maxReportedExtentSize:
                (result , partoutput) = win32file.ReadFile(self.__hfile,self.__maxReportedExtentSize,None)
                output = output + partoutput
                size = size - self.__maxReportedExtentSize
            (result , partoutput) = win32file.ReadFile(self.__hfile,size,None)
            output = output + partoutput
        except Exception as ex:
            raise FileException(filename , ex)
        return output

    def lock(self):
        filename = self.__volumeName
        try:
            outbuffer = win32file.DeviceIoControl(self.__hfile,  winioctlcon.FSCTL_LOCK_VOLUME,  None, None, None )
        except Exception as ex:
            raise FileException(filename , ex)
    

    