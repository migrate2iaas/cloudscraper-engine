

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

class AllFilesIterator(object):
    """Iterator for looping over a sequence backwards."""

    #Members
    #__rootIterator = None
    #__currentIterator = None
    #__RootPath = None

    def __init__(self, rootIterator, rootPath):
        self.__RootPath = rootPath
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
            currentpath = self.__RootPath+'\\'+data[8]
            self.__currentIterator = AllFilesIterator(win32file.FindFilesIterator(currentpath+'\\*', None ) , currentpath)
            return self.next()
        else:
            #data[8] == filename
            return self.__RootPath+'\\'+data[8]

#
class VolumeExtent(object):
    # members:
   # __startInBytes = long(0)
   # __sizeInBytes = long(0)
    def __init__(self, start , size):
        self.__startInBytes = start
        self.__sizeInBytes = size

    def getSize(self):
        return self.__sizeInBytes
    def getStart(self):
        return self.__startInBytes

    def __str__(self):
        return "["+str(self.__startInBytes)+str(self.__startInBytes+self.__sizeInBytes)+")"

#
#
class WindowsVolume(object):
    """Windows volume"""
    
    # Class properties:
    #__hfile = None
    #__volumeName = None
    #__filesystem = None
    #__bytesPerCluster = None

    def __init__(self,volumeName):
       
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        
        self.__hfile = win32file.CreateFile( volumeName, win32con.GENERIC_READ | ntsecuritycon.FILE_READ_ATTRIBUTES | ntsecuritycon.FILE_WRITE_ATTRIBUTES, win32con. FILE_SHARE_READ|win32con.FILE_SHARE_WRITE, secur_att,   win32con.OPEN_ALWAYS, win32con.FILE_ATTRIBUTE_NORMAL , 0 )
        self.__volumeName = volumeName
        

        output_buffer_size = 256;
        outbuffer = win32file.DeviceIoControl(self.__hfile,  winioctlcon.FSCTL_GET_NTFS_VOLUME_DATA,  None, output_buffer_size, None )
       
        #     typedef struct {
        #  LARGE_INTEGER VolumeSerialNumber;
        #  LARGE_INTEGER NumberSectors;
        #  LARGE_INTEGER TotalClusters;
        #  LARGE_INTEGER FreeClusters;
        #  LARGE_INTEGER TotalReserved;
        #  DWORD         BytesPerSector;
        #  DWORD         BytesPerCluster;
        #  DWORD         BytesPerFileRecordSegment;
        #  DWORD         ClustersPerFileRecordSegment;
        #  LARGE_INTEGER MftValidDataLength;
        #  LARGE_INTEGER MftStartLcn;
        #  LARGE_INTEGER Mft2StartLcn;
        #  LARGE_INTEGER MftZoneStart;
        #  LARGE_INTEGER MftZoneEnd;
        #} NTFS_VOLUME_DATA_BUFFER, *PNTFS_VOLUME_DATA_BUFFER;

        if outbuffer:
            self.__bytesPerCluster = struct.unpack("@qqqqqIIIIqqqqq",outbuffer[0:96])[6]

            self.__filesystem = 'NTFS';

        return 

    # gets the volume size
    def getVolumeSize(self):
            # typedef struct {
            #LARGE_INTEGER Length;
            #} GET_LENGTH_INFORMATION;
        IOCTL_DISK_GET_LENGTH_INFO = 0x7405c
        outbuffersize = 8
        outbuffer = win32file.DeviceIoControl(self.__hfile, winioctlcon.IOCTL_DISK_GET_LENGTH_INFO , None , outbuffersize , None )
        return struct.unpack("@q" , outbuffer)[0]

    # gets the file enumerator (iterable). each of it's elements represent a filename (unicode string)
    def getFileEnumerator(self):
        return AllFilesIterator(win32file.FindFilesIterator(self.__volumeName+'\\*', None ) , self.__volumeName)

    
    # returns iterable of VolumeExtent structs
    def getFileDataBlocks(self, fileName):
        secur_att = win32security.SECURITY_ATTRIBUTES()
        secur_att.Initialize()
        hFile = win32file.CreateFile( self.__volumeName+"\\"+fileName,ntsecuritycon.FILE_READ_ATTRIBUTES, win32con.FILE_SHARE_READ|win32con.FILE_SHARE_WRITE|win32con.FILE_SHARE_DELETE, secur_att,   win32con.OPEN_ALWAYS, win32con.FILE_ATTRIBUTE_NORMAL , 0 )

        # input
        # STARTING_VCN_INPUT_BUFFER
        #typedef struct {
        #LARGE_INTEGER StartingVcn;
        #} STARTING_VCN_INPUT_BUFFER, *PSTARTING_VCN_INPUT_BUFFER;
        #
        vcn_input_buffer = struct.pack('=q',0)

        #TODO: check on really big files
        vcn_ouput_buffer_size = 64*1024*1024

        #TODO: handle if buffer was too small
        outbuffer = win32file.DeviceIoControl(hFile,  winioctlcon.FSCTL_GET_RETRIEVAL_POINTERS,  vcn_input_buffer, vcn_ouput_buffer_size, None )

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
        volextents = list();
        extenttuple = namedtuple('Extents', 'NextVcn Lcn')
        while extent_count < retrieval_pointers_buffer_header.ExtentCount:
            
            extent = extenttuple._make(struct.unpack_from("@qq", outbuffer[buffer_offset:buffer_offset+struct.calcsize("@qq")]))
           
            volextent = VolumeExtent(extent.Lcn * self.__bytesPerCluster, (extent.NextVcn - current_file_vcn) * self.__bytesPerCluster )
            volextents.append(volextent)

            buffer_offset = buffer_offset + struct.calcsize("@qq")
            extent_count = extent_count + 1

        #TODO: maybe to make kinda dynamic list requesting each next vacb when iterated?
        return volextents

    #gets filesystem string 
    def getFileSystem(self):
        return self.__filesystem

    #returns iterable of filled bitmap extents
    def getFilledVolumeBitmap(self):
        
        # should be enough to hold a bitmap for 2 Tb disk
        bitmap_out_buffer_size = 64*1024*1024

        # input
        # STARTING_VCN_INPUT_BUFFER
        #typedef struct {
        #LARGE_INTEGER StartingVcn;
        #} STARTING_VCN_INPUT_BUFFER, *PSTARTING_VCN_INPUT_BUFFER;
        # should be 0 for the overall bitmap
        vcn_input_buffer = struct.pack('=q',0)

        outbuffer = win32file.DeviceIoControl(self.__hfile,  winioctlcon.FSCTL_GET_VOLUME_BITMAP,  vcn_input_buffer, bitmap_out_buffer_size, None )
        
        #typedef struct {
        #LARGE_INTEGER StartingLcn;
        #LARGE_INTEGER BitmapSize;
        #BYTE          Buffer[1];
        #} VOLUME_BITMAP_BUFFER, *PVOLUME_BITMAP_BUFFER;
        volume_bitmap_buffer_headertuple = namedtuple('VOLUME_BITMAP_BUFFER', 'StartingLcn BitmapSize')
        volume_bitmap_buffer_header = volume_bitmap_buffer_headertuple._make(struct.unpack("@qq" , outbuffer[0:struct.calcsize("@qq")]))
        
        bits_left = volume_bitmap_buffer_header.BitmapSize;
        current_start = volume_bitmap_buffer_header.StartingLcn;
        buffer_offset = struct.calcsize("@qq")
        last_start = long(0);
        last_size = long(0);
        volextents = list()
        
        while bits_left > 0:
            byte = struct.unpack_from("=B", outbuffer[buffer_offset:buffer_offset+1])
            # our granularity is 8 clusters, mark all used even if one of 8 is used
            if byte > 0: 
                if last_size == 0:
                    last_start = current_start;
                last_size = last_size + 8 * self.__bytesPerCluster;    
            else: 
                if last_size:
                    volextent = VolumeExtent(last_start, last_size)
                    volextents.append(volextent)
                last_size = 0;
                last_start = 0;

            bits_left = bits_left - 8;
            buffer_offset = buffer_offset + 1
            current_start += 8*self.__bytesPerCluster;
        
        if last_size:
            volextent = VolumeExtent(last_start, last_size)
            volextents.append(volextent)
        

        return volextents

    #returns bytes read
    def readExtent(self, volextent):
        win32file.SetFilePointer(self.__hfile, volextent.getStart(), win32con.FILE_BEGIN)
        (result , output) = win32file.ReadFile(self.__hfile,volextent.getSize(),None)
        return output

    