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
eb 63 90 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 80 01 00 00 00 \
00 00 00 00 ff fa eb 05 f6 c2 80 74 05 f6 c2 70 \
74 02 b2 80 ea 79 7c 00 00 31 c0 8e d8 8e d0 bc \
00 20 fb a0 64 7c 3c ff 74 02 88 c2 52 bb 17 04 \
f6 07 03 74 06 be 88 7d e8 17 01 be 05 7c b4 41 \
bb aa 55 cd 13 5a 52 72 3d 81 fb 55 aa 75 37 83 \
e1 01 74 32 31 c0 89 44 04 40 88 44 ff 89 44 02 \
c7 04 10 00 66 8b 1e 5c 7c 66 89 5c 08 66 8b 1e \
60 7c 66 89 5c 0c c7 44 06 00 70 b4 42 cd 13 72 \
05 bb 00 70 eb 76 b4 08 cd 13 73 0d 5a 84 d2 0f \
83 d0 00 be 93 7d e9 82 00 66 0f b6 c6 88 64 ff \
40 66 89 44 04 0f b6 d1 c1 e2 02 88 e8 88 f4 40 \
89 44 08 0f b6 c2 c0 e8 02 66 89 04 66 a1 60 7c \
66 09 c0 75 4e 66 a1 5c 7c 66 31 d2 66 f7 34 88 \
d1 31 d2 66 f7 74 04 3b 44 08 7d 37 fe c1 88 c5 \
30 c0 c1 e8 02 08 c1 88 d0 5a 88 c6 bb 00 70 8e \
c3 31 db b8 01 02 cd 13 72 1e 8c c3 60 1e b9 00 \
01 8e db 31 f6 bf 00 80 8e c6 fc f3 a5 1f 61 ff \
26 5a 7c be 8e 7d eb 03 be 9d 7d e8 34 00 be a2 \
7d e8 2e 00 cd 18 eb fe 47 52 55 42 20 00 47 65 \
6f 6d 00 48 61 72 64 20 44 69 73 6b 00 52 65 61 \
64 00 20 45 72 72 6f 72 0d 0a 00 bb 01 00 b4 0e\
cd 10 ac 3c 00 75 f4 c3 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 \
00 00 00 00 00 00 00 00 00 00 00 00 00 00 55 aa'


    def __init__(self , backing_store , mbr_id = int(random.randint(1, 0x0FFFFFFF)), default_offset = 0x800*0x200 , windows = True):
        self.__backingStore = backing_store
        self.__freeSize = self.__backingStore.getSize()
        self.__wholeSize = self.__backingStore.getSize()
        self.__mbrId = int(mbr_id)
        self.__grubPath = "../resources/boot/grub/core.img"

        self.__windows = windows
        #NOTE: alternatively, these parms could be re-loaded from backing store
        #TODO: make the reload
        if windows:
            self.__grub = False
            builtin_mbr_hex_str = SimpleDiskParser.nt_mbr_hex_str
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
        mbr[0x1be-6:0x1be-2]=struct.pack('=I',self.__mbrId)
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
        mbr[partentry+8:partentry+0xc] = struct.pack('=I',sectoroffset)
        #the size 
        mbr[partentry+0xc:partentry+0x10] = struct.pack('=I',int(size/0x200))
        
        #NOTE: nevertheless we track it several volumes on the same disk case was not tested
        
        #write mbr
        ext = DataExtent.DataExtent(0 , 0x200)
        ext.setData(mbr)
        self.writeRawMetaData(ext)

        self.__mbr = mbr
        self.__currentOffset = self.__currentOffset + size
        self.__partitionsCreated = self.__partitionsCreated + 1

        #write grub image
        if self.__windows == False:
            grubfile = open(self.__grubPath, "rb")
            grubdata = grubfile.read()
            #PAD to sector size
            while len(grubdata) % 512:
                grubdata += '\0'
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
