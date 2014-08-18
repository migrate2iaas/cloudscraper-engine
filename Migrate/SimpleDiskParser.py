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

    builtin_mbr_hex_str = '\
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


    def __init__(self , backing_store , mbr_id = int(random.randint(1, 0x0FFFFFFF)), default_offset = 0x800*0x200):
        self.__backingStore = backing_store
        self.__freeSize = self.__backingStore.getSize()
        self.__wholeSize = self.__backingStore.getSize()
        self.__mbrId = int(mbr_id)
        #NOTE: alternatively, these parms could be re-loaded from backing store
        #TODO: make the reload
        self.__mbr = bytearray.fromhex(SimpleDiskParser.builtin_mbr_hex_str)
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

    # the disk is formated and new volume is generated
    def createTransferTarget(self, size, fix_nt_boot = True):
        #TODO: test self.__partitionsCreated < 4
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
        mbr[partentry+4] = 0x7
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
        
        ext = DataExtent.DataExtent(0 , 0x200)
        ext.setData(mbr)
        self.writeRawMetaData(ext)
        
        self.__mbr = mbr
        self.__currentOffset = self.__currentOffset + size
        self.__partitionsCreated = self.__partitionsCreated + 1

        return SimpleTransferTarget.SimpleTransferTarget(sectoroffset*0x200 , self.__backingStore , fix_nt_boot)

    

    # to write directly the partitioning schemes
    def writeRawMetaData(self, metadataExtents):
        self.__backingStore.writeMetadata(metadataExtents)

    # to read the partitioning schemes
    #NOTE: not sure if it works
    def readRawMetaData(self, metadataExtent):
        return self.__backingStore.readMetadata(metadataExtent)
