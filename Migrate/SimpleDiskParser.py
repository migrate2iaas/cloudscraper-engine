# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import sys

import subprocess
import re
import random
import struct
import logging

import DataExtent
import SimpleTransferTarget




#TODO: to implement base class
class SimpleDiskParser(object):
    """The simpliest MBR container parser"""

    nt_mbr_hex_str = '\
fa ea 06 00 c0 07 8c c8 \
8e d8 8e d0 bc fc ff fb \
b0 53 e8 e2 00 b8 a0 07 \
8e c0 31 f6 31 ff b9 00 \
02 fc f2 a4 ea 29 00 a0 \
07 b0 52 e8 c9 00 1e 07 \
0e 1f f6 c2 80 75 0b 66 \
be 13 01 00 00 e8 ab 00 \
b2 80 66 be be 01 00 00 \
66 b9 04 00 00 00 b0 4c \
e8 a4 00 8a 44 00 3c 80 \
74 18 66 83 c6 10 e2 ee \
66 be 3c 01 00 00 e8 82 \
00 fa f4 b0 2e e8 87 00 \
eb f7 b0 42 e8 80 00 8b \
14 8b 4c 02 66 b8 01 02 \
00 00 31 db cd 13 73 13 \
80 fa 80 75 aa 66 be 2f \
01 00 00 e8 55 00 e8 33 \
00 eb ce b0 43 e8 57 00 \
66 31 c0 66 bb fe 01 00 \
00 67 8b 03 66 3d 55 aa \
00 00 74 0b 66 be 52 01 \
00 00 e8 2e 00 eb aa b0 \
47 e8 33 00 66 ea 00 7c \
00 00 00 00 50 53 66 bb \
03 01 00 00 50 88 e0 66 \
83 e0 0f d7 e8 18 00 58 \
66 83 e0 0f d7 e8 0f 00 \
5b 58 c3 50 fc ac 84 c0 \
74 0f e8 02 00 eb f6 50 \
53 b4 0e 31 db 43 cd 10 \
5b 58 c3 30 31 32 33 34 \
35 36 37 38 39 41 42 43 \
44 45 46 4d 42 52 20 6f \
6e 20 66 6c 6f 70 70 79 \
20 6f 72 20 6f 6c 64 20 \
42 49 4f 53 0d 0a 00 52 \
65 61 64 20 65 72 72 6f \
72 0d 0a 00 4e 6f 20 61 \
63 74 69 76 65 20 70 61 \
72 74 69 74 69 6f 6e 0d \
0a 00 49 6e 76 61 6c 69 \
64 20 53 69 67 6e 61 74 \
75 72 65 0d 0a 00 90 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 55 aa \
'

#Grub boot code
    grub_mbr_hex_str = '\
63 eb 00 90 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 80 00 00 01 00 00 \
00 00 00 00 fa ff 90 90 c2 f6 74 80 f6 05 70 c2 \
02 74 80 b2 79 ea 00 7c 31 00 8e c0 8e d8 bc d0 \
20 00 a0 fb 7c 64 ff 3c 02 74 c2 88 bb 52 04 17 \
07 f6 74 03 be 06 7d 88 17 e8 be 01 7c 05 41 b4 \
aa bb cd 55 5a 13 72 52 81 3d 55 fb 75 aa 83 37 \
01 e1 32 74 c0 31 44 89 40 04 44 88 89 ff 02 44 \
04 c7 00 10 8b 66 5c 1e 66 7c 5c 89 66 08 1e 8b \
7c 60 89 66 0c 5c 44 c7 00 06 b4 70 cd 42 72 13 \
bb 05 70 00 76 eb 08 b4 13 cd 0d 73 84 5a 0f d2 \
d0 83 be 00 7d 93 82 e9 66 00 b6 0f 88 c6 ff 64 \
66 40 44 89 0f 04 d1 b6 e2 c1 88 02 88 e8 40 f4 \
44 89 0f 08 c2 b6 e8 c0 66 02 04 89 a1 66 7c 60 \
09 66 75 c0 66 4e 5c a1 66 7c d2 31 f7 66 88 34 \
31 d1 66 d2 74 f7 3b 04 08 44 37 7d c1 fe c5 88 \
c0 30 e8 c1 08 02 88 c1 5a d0 c6 88 00 bb 8e 70 \
31 c3 b8 db 02 01 13 cd 1e 72 c3 8c 1e 60 00 b9 \
8e 01 31 db bf f6 80 00 c6 8e f3 fc 1f a5 ff 61 \
5a 26 be 7c 7d 8e 03 eb 9d be e8 7d 00 34 a2 be \
e8 7d 00 2e 18 cd fe eb 52 47 42 55 00 20 65 47 \
6d 6f 48 00 72 61 20 64 69 44 6b 73 52 00 61 65 \
00 64 45 20 72 72 72 6f 0a 0d bb 00 00 01 0e b4 \
10 cd 3c ac 75 00 c3 f4 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 55 aa'


    def __init__(self , backing_store , mbr_id = int(random.randint(1, 0x0FFFFFFF)), default_offset = 0x800*0x200 , windows = True):
        self.__backingStore = backing_store
        self.__freeSize = self.__backingStore.getSize()
        self.__wholeSize = self.__backingStore.getSize()
        self.__mbrId = int(mbr_id)
        self.__grubPath = "..\\resources\\boot\\grub\\core.img"
        #NOTE: alternatively, these parms could be re-loaded from backing store
        #TODO: make the reload
        if windows:
            self.__grub = False
            builtin_mbr_hex_str = SimpleDiskParser.builtin_mbr_hex_str
        else:
            self.__grub = True
            builtin_mbr_hex_str = SimpleDiskParser.grub_mbr_hex_str

        try:
            self.__mbr = bytearray.fromhex(builtin_mbr_hex_str)
        except TypeError:
            # Work-around for Python 2.6 bug 
            self.__mbr = bytearray.fromhex(unicode(builtin_mbr_hex_str))
        
        self.__defaultOffset = default_offset
        self.__currentOffset = self.__defaultOffset
        self.__partitionsCreated = 0

    #TODO: enum of existing targets\volumes

    #pragma pack(push, 1)
#typedef struct 
#{ // partition record
#  UCHAR indicator;    // 00 boot indicator (80 = active partition)
#  UCHAR starthead;    // 01 start head
#  UCHAR startsec;     // 02 bits 0-5: start sector, bits 6-7: 
#                     //    bits 8-9 of start track
#  UCHAR starttrack;   // 03 bits 0-7 of start track
#  UCHAR parttype;     // 04 partition type
#  UCHAR endhead;	     // 05 end head
#  UCHAR endsec;       // 06 end sector
#  UCHAR endtrack;     // 07 end track
#  ULONG bias;        // 08 sector bias to start of partition
#  ULONG partsize;    // 0C partition size in sectors
#} PARTITION,*PPARTITION;
#pragma pack(pop)
    PART_TYPE_NTFS = 0x07
    PART_TYPE_EXT = 0x83
    
    def createTransferTarget(self, size, fix_nt_boot = True , part_type = 0x07):
        """
            Formats disk, generates new volume 
        """
        mbr = self.__mbr
        sectoroffset = self.__currentOffset/0x200
        #set the mbr-id
        mbr[0x1be-6:0x1be-2]=struct.pack('=i',self.__mbrId)
        #part entry offset
        partentry = 0x1be + self.__partitionsCreated * 0x10
        mbr[partentry] = 0x80 # boot indicator
        # create starting offsets as they default value in Win2008
        mbr[partentry+1] = 0x20
        mbr[partentry+2] = 0x21
        mbr[partentry+3] = 0x0
        mbr[partentry+4] = part_type
        #end for lba-mode
        mbr[partentry+5] = 0xFE
        mbr[partentry+6] = 0xFF
        mbr[partentry+7] = 0xFF
        #the standeard offset is 0x0800 sectors (1Mb)
        mbr[partentry+8:partentry+0xc] = struct.pack('=i',sectoroffset)
        #the size 
        #NOTE: error is reported when it's more than 1TB
        mbr[partentry+0xc:partentry+0x10] = struct.pack('=i',int(size/0x200))
        
        #NOTE: nevertheless we track it several volumes on the same disk case was not tested
        
        #write mbr
        ext = DataExtent.DataExtent(0 , 0x200)
        ext.setData(mbr)
        self.writeRawMetaData(ext)

        self.__mbr = mbr
        self.__currentOffset = self.__currentOffset + size
        self.__partitionsCreated = self.__partitionsCreated + 1

        #write grub image
        grubfile = open(self.__grubPath, "rb")
        grubdata = grubfile.read()
        ext = DataExtent.DataExtent(0x200 , len(grubdata))
        ext.setData(grubdata)
        self.writeRawMetaData(ext)

        return SimpleTransferTarget.SimpleTransferTarget(sectoroffset*0x200 , self.__backingStore , fix_nt_boot)

    

    # to write directly the partitioning schemes
    def writeRawMetaData(self, metadataExtents):
        self.__backingStore.writeMetadata(metadataExtents)

    # to read the partitioning schemes
    #NOTE: not sure if it works
    def readRawMetaData(self, metadataExtent):
        return self.__backingStore.readMetadata(metadataExtent)
