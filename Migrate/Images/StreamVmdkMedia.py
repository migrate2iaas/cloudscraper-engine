#created by Robin van der Veer
#18-07-2014

import ImageMedia

import struct
import sys
import os
import math
import string
import zlib
import logging

# Header Constants
MAGIC_NUMBER = 0x564D444B # 'V' 'M' 'D' 'K'
EXPECTED_FLAGS = 196609 #bits 0, 16 and 17
EXPECTED_VERSION = 3
EXPECTED_COMPRESS_ALGORITHM = 1
EXPECTED_GTE_PER_GT = 512

# Marker Constants
MARKER_EOS = 0 # end of stream
MARKER_GT = 1 # grain table
MARKER_GD = 2 # grain directory
MARKER_FOOTER = 3 # footer (repeat of header with final info)

# Other Constants
SECTOR_SIZE = 512
SECTORS_PER_GRAIN = 128
GRAIN_SIZE = SECTOR_SIZE * SECTORS_PER_GRAIN
UINT32_BYTE_SIZE = 4 #makes sence, but improves readability
UINT64_BYTE_SIZE = 8
DESCRIPTOR_SIZE = 3 #this will always be enough

# Descriptor Template
#Although spec says all line starting with # are comments, the first line MUST be either
# '# Disk DescriptorFile'
# or
# '# Disk Descriptor File'
# as indicated by lines 1829 and 1830 in VMDK.cpp
#Also: there MUST be a space after the = (line 1411 VMDK.cpp)
#Also: the enrties must NOT start with a space/tab or any other char (line 1409 VMDK.cpp)
image_descriptor_template= \
'''# Disk DescriptorFile
version=1
CID=7e5b80a7
parentCID=ffffffff
createType= "streamOptimized"

# Extent description
RDONLY #SECTORS# SPARSE "#FILEPATH#"

# The Disk Data Base 
#DDB

ddb.virtualHWVersion = "4"
ddb.adapterType = "ide"
ddb.geometry.cylinders = "#CYLINDERS#"
ddb.geometry.heads = "255"
ddb.geometry.sectors = "63"
# Believe this is random
ddb.longContentID = "8f15b3d0009d9a3f456ff7b28d324d2a"
'''

class VMDKStreamException(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg


class ParsedStreamOptimizedHeader(object):
    def __init__(self, rawHeader = ""):
        super(ParsedStreamOptimizedHeader,self).__init__() 
        if rawHeader != "":
            temp = struct.unpack("=IIIQQQQIQQQBccccH",rawHeader[:79])
            self.magicNumber = temp[0]
            self.version = temp[1]
            self.flags = temp[2]
            self.capacity = temp[3]
            self.grainSize = temp[4]
            self.descriptorOffset = temp[5]
            self.descriptorSize = temp[6]
            self.numGTEsPerGT = temp[7]
            self.rgdOffset = temp[8]
            self.gdOffset = temp[9]
            self.overHead = temp[10]
            self.uncleanShutdown = temp[11]
            self.singleEndLineChar = temp[12]
            self.nonEndLineChar = temp[13]
            self.doubleEndLineChar1 = temp[14]
            self.doubleEndLineChar2 = temp[15]
            self.compressAlgorithm = temp[16]
        else:
            self.magicNumber = MAGIC_NUMBER
            self.version = EXPECTED_VERSION
            self.flags = EXPECTED_FLAGS
            self.capacity = 0 #This is a 'fresh' header/footer: set size to 0 (will be updated after writes)
            self.grainSize = SECTORS_PER_GRAIN
            self.descriptorOffset = 1 #Right after this header
            self.descriptorSize = DESCRIPTOR_SIZE #We don't know it's exact final length yet, but we do already need to write it. So we set it to a size that deffinitly long enough
            self.numGTEsPerGT = EXPECTED_GTE_PER_GT
            self.rgdOffset = 0
            self.gdOffset = 0xFFFFFFFFFFFFFFFF #initial value
            self.overHead = 0 #NOTE: nowhere is it mentioned this must be 128 (which was mentioned in the original github project), I like to just set it to 0(seem to be completely irrelevant for stream optimized format)
            self.uncleanShutdown = 0 #fresh header
            self.singleEndLineChar = '\n'
            self.nonEndLineChar = ' '
            self.doubleEndLineChar1 = '\r'
            self.doubleEndLineChar2 = '\n'
            self.compressAlgorithm = EXPECTED_COMPRESS_ALGORITHM
            self.verifyHeader()
        
    
    #verify that the header has the correct/expected values for a number of fields
    def verifyHeader(self):
        errorDetected = False
        if self.magicNumber != MAGIC_NUMBER:
            errorDetected = True
            logging.critical("invalid/corrupted input file! (incorrect magicNumber)")
        if self.flags != EXPECTED_FLAGS:
            errorDetected = True
            logging.critical("invalid/corrupted input file! (incorrect flags)")
        if self.version != EXPECTED_VERSION:
            errorDetected = True
            logging.critical("invalid/corrupted input file! (incorrect version)")
        if self.capacity % self.grainSize != 0:
            errorDetected = True
            logging.critical("invalid/corrupted input file! (capacity not a multiple of grainSize)")
        if self.uncleanShutdown != 0:
            logging.warning("uncleanShutdown detected!")
        if self.compressAlgorithm != EXPECTED_COMPRESS_ALGORITHM:
            errorDetected = True
            logging.critical("invalid/corrupted input file! (incorrect compressAlgorithm)")
        if self.numGTEsPerGT != EXPECTED_GTE_PER_GT:
            errorDetected = True
            logging.critical("invalid/corrupted input file! (incorrect GT entries per GT)")
        if self.grainSize != SECTORS_PER_GRAIN:
            logging.warning("Unexpected grainSize (!=128)")
        if errorDetected:
            raise VMDKStreamException("File is of wrong format or corrupted")
    
    #convert this header to a raw header that can be written to the file
    def toRawHeader(self):
        header = [self.magicNumber, self.version, self.flags, self.capacity, self.grainSize, self.descriptorOffset, \
                    self.descriptorSize, self.numGTEsPerGT, self.rgdOffset, self.gdOffset, self.overHead, self.uncleanShutdown, \
                    self.singleEndLineChar, self.nonEndLineChar, self.doubleEndLineChar1, self.doubleEndLineChar2, self.compressAlgorithm]
        for i in range(433):
	        header.append(0)
        return struct.pack("=IIIQQQQIQQQBccccH433B", *header)
        
    
    #input: a header and a footer (both are assumed to be verified)
    #output: True if the two are a valid header/footer pair, false otherwise
    @staticmethod
    def validHeaderFooterPair(header, footer):
        if header.capacity != footer.capacity or \
            header.grainSize != footer.grainSize or \
            header.descriptorOffset != footer.descriptorOffset or \
            header.descriptorSize != footer.descriptorSize or \
            header.numGTEsPerGT != footer.numGTEsPerGT or \
            header.rgdOffset != footer.rgdOffset or \
            header.overHead != footer.overHead or \
            header.uncleanShutdown != footer.uncleanShutdown:
            return False
        return True

class GrainMarker:
    def __init__(self, fileOffset, diskOffset, isNull):
        self.fileOffset = fileOffset
        self.diskOffset = diskOffset
        self.isNull = isNull

#round up divide
def divro(num, den):
    return int(math.ceil((1.0*num)/(1.0*den)))


        
class StreamVmdkMedia(ImageMedia.ImageMedia):
    """Media representing the stream-optimized VMDK media format"""
    __zeroGrain = ""
    __zeroGT = []
    __initialized = False

    def __init__(self, filePath, size = -1, bufferSize = GRAIN_SIZE * 100 , compression = 6):
        """ constructor
            Args:
                filePath: path to the image the be created/read
                size: (only used when creating a new image!) (NOTE: SIZE IN BYTES!)
                   if not specified (-1): this disk will just grow as much as is necacery to hold all written data
                   if specified (>0): this disk will have a fix size, and writing too much data to it will raise an exception
                compression: from (0 to 9) zlib compression rate. Lower rates will use fewer CPU
            Throws: 
                ValueError:
                    if size specified and size != -1 and size<=0
                    if size specified and it is not an integer multiple of GRAIN_SIZE
                
                """
        if not StreamVmdkMedia.__initialized: #if we did not yet initialize the static members
            StreamVmdkMedia.__init()
        if size <= 0 and size != -1:
            raise ValueError("Size must either be -1 or >0")
        if size != -1 and size % GRAIN_SIZE:
            raise ValueError("Size must be integer multiple of GRAIN_SIZE")
        self.__size = size
        self.__filePath = filePath
        self.__opened = False
        self.__bufferSize = bufferSize
        self.__compression = compression

        
        
            
    def open(self):
        """ Opens the image. Required before any operation related to image can be done
            initializes all data structures required for other operations
            Throws:
                VMDKStreamException:
                    if existing file is opened and it corrupted
                    if new file is created and for whatever reason the initialized header is not valid (should never happen)"""
        if self.__opened:
            return None
        if os.path.isfile(self.__filePath):
            logging.warning("Image already exists: opening in read only mode")
            self.__openExisting()
        else:
            self.__createOpenNew()
        self.__opened = True
     
         
    def getMaxSize(self):
        """ Returns the virtual disks size in bytes. Note: it couldn't be called before open() but could be called after close()
            For newly created image:
                if created with size = -1: returns highest offset that has been written to, round up to nearest multiple of GRAIN_SIZE
                if created with size > 0: returns size
            For image that already existed:
                returns image size (read form header)  
            Throws:
                """
        
       # if not self.__opened:
       #     raise VMDKStreamException("cannot get disk size: image not opened")
            
        if self.__readOnly:
            return self.__parsedHeader.capacity * SECTOR_SIZE    
        if self.__size == -1:
            size = self.__writtenDataRawLength
            if len(self.__incompleteWrittenGrain) != 0:
                size += GRAIN_SIZE
            return size
        return self.__size
    
    def getImageSize(self):
        """ returns image file size in bytes.
            flushes the file first"""
        #if not self.__opened:
        #    raise VMDKStreamException("cannot get image size: image not opened")
        self.flush()
        return os.path.getsize(self.__filePath)
    
    def flush(self):
        """ flush data to the file 
            Throws:
                VMDKStreamException
                    if image is not yet opened"""
        if not self.__opened:
            return
            #raise VMDKStreamException("cannot flush: image not opened")
        if self.__readOnly: #if we are in read only mode, flushing the file does nothing
            return
        self.__file.flush()
    
    def reopen(self):
        """ reopens the image file. Note that this will always result in a read-only mode """
        self.close()
        self.open()
    
    def readImageData(self, offset, size):
        """ Read data from the image file:
            args:
                offset: the offset in bytes from which the data has to be read
                size: the amount of byte we have to read
            Throws:
                VMDKStreamException
                    if image is not yet opened
                    if offset + size > self.getImageSize(): trying to read past end of file
                     """
       # if not self.__opened:
       #     raise VMDKStreamException("cannot read: image not opened")
        if offset > self.getImageSize():
            raise VMDKStreamException("Trying to read past end of file")
        if not self.__readOnly and offset < SECTOR_SIZE + self.__parsedHeader.descriptorSize * SECTOR_SIZE:
            logging.warning("Reading from part of file that is still subject to change! the first %s bytes of the file will be overwritten on file close. Using this data to copy this disk WILL result in a corrupted file"%(SECTOR_SIZE + self.__parsedHeader.descriptorSize * SECTOR_SIZE))
        
        #if the file is opened, but the file is closed already, than close() has been called.
        #this means we have to make sure that we leave the file closed when we are done to avoid resource leaking
        closeWhenDone = False
        if self.__file.closed:
            closeWhenDone = True
            self.__reopenFile()
        
        
        self.__file.seek(offset)
        data = self.__file.read(size)
        
        if closeWhenDone:
            self.__file.close()
        return data
        
    
    def readDiskData(self, offset, size):
        """ Read data from the virtual disk:
        args:
            offset: the offset in bytes from which the data has to be read
            size: the amount of byte we have to read 
        Throws:
            VMDKStreamException
                if image is not yet opened
                if size + offset > self.getMaxSize(): trying to read past end of disk
                if a corrupted/invalid grain is read from disk
        """       
        if not self.__opened:
            raise VMDKStreamException("cannot read: image not opened")
        #if the file is opened, but the file is closed already, than close() has been called.
        #this means we have to make sure that we leave the file closed when we are done to avoid resource leaking
        closeWhenDone = False
        if self.__file.closed:
            closeWhenDone = True
            self.__reopenFile() 
        
        return self.__readDiskData(offset, size)#don't expose the check argument; should not be used by user.
        
        if closeWhenDone:
            self.__file.close()
    
    def __readDiskData(self, offset, size, check = True):
        """ internaly used. the check argument must be used for preset size initialized images. 
        """
        if size % SECTOR_SIZE != 0:
            logging.warning("requesting a read with size%SECTOR_SIZE != 0. Will read as if size rouned up to nearest SECTOR_SIZE")
            size += SECTOR_SIZE - size % SECTOR_SIZE
        if size + offset > self.getMaxSize():
            raise VMDKStreamException("Trying to read past end of disk")
        if size == 0:#reduces the need to tedious error checking later on
            return ""

        
        #We are going to read from a virtual disk that is still being written too, and which' size was set on creation
        #this means that we might want to read data from part of the disk that is not yet written to, and for which no
        #GT entries exist. So we have to manually add padding to fill up not-yet-existing data
        if not self.__readOnly and self.__size != -1 and check:
            data = self.__readDiskData(offset, min(self.__writtenDataRawLength - offset, size), False)#We first recursively read as much data as 
                                                                    #possible the normal way
            data = data[:min(self.__writtenDataRawLength - offset, size)]#than we remove any padding that may have been added because we
                                                                    #did not read a full sector multiple
            offset += len(data)
            size -= len(data)
            data += self.__incompleteWrittenGrain[: size]#add data from incomplete grain
            while len(data) != size: #and than add padding for the not yet writting to part of the disk that we tried to read from
                data += "\0"
            return data
        
        readData = ""
        ##READ (part of) FIRST GRAIN##
        sectorOffset = StreamVmdkMedia.__byteOffsetToSectorOffset(offset) #translate the offset in bytes to an offset in sectors
        grainOffset = StreamVmdkMedia.__sectorOffsetToGrainOffset(sectorOffset)
        offsetInGrain = offset - grainOffset * GRAIN_SIZE #offset in the first grain we are going to read from
        read = self.__readGrain(offset)
        readData += read[offsetInGrain:offsetInGrain + size]
        offset += len(readData) #update offset for further reads
        
        #Now we can keep reading grains until we need <1 stil
        while (size - len(readData)) >= GRAIN_SIZE:
            read = self.__readGrain(offset)
            readData += read
            offset += GRAIN_SIZE#We now have to continue reading from a new offset (and we are sure that we read one whole grain of data)
            
        #Add the last bit of data we still need:
        if len(readData) != size:
            read = self.__readGrain(offset)
            readData += read[: size - len(readData)]

        
        return readData
    
    def __reopenFile(self):
        self.__file = open(self.__filePath, "rb")
    
    def __readGrain(self, offset):
        """ Read one grain of data from the disk """
        sectorOffset = StreamVmdkMedia.__byteOffsetToSectorOffset(offset) #translate the offset in bytes to an offset in sectors
        grainOffset = StreamVmdkMedia.__sectorOffsetToGrainOffset(sectorOffset)
        
        if grainOffset == len(self.__fullGT):
            return self.__incompleteWrittenGrain + StreamVmdkMedia.__padToGrain(self.__incompleteWrittenGrain)
        fileLocation = self.__fullGT[ grainOffset ] * SECTOR_SIZE#get the location in the file where we can find the grain
        
        if fileLocation:
            self.__file.seek( fileLocation + UINT64_BYTE_SIZE)#set the file position to point to the data-length byte of the marker
            compressedLength = struct.unpack("=I", self.__file.read(UINT32_BYTE_SIZE))[0]#extract the required number of bytes
            compressedData = self.__file.read( compressedLength )#read the compressed data
            uncompressedData = zlib.decompress(compressedData)
            if len(uncompressedData) != GRAIN_SIZE:
                logging.critical("len(Uncompressed grain) != GRAIN_SIZE")
                raise VMDKStreamException("invalid/corrupted input file! (incorrect grain size)")
            return uncompressedData#and since we still need to read at least a whole grain we can add all uncompressed data
        else:#null block: add one whole grain of nulls
            return StreamVmdkMedia.__zeroGrain
     
    def writeDiskDataPath(self, offset, filePath):
        """ Can be used to write large file to disk
            args:
                offset: offset in byte on which to write the file
                filePath: path to file that is to be written to the disk
            Throws:
                VMDKStreamException
                    if image not opened yet
                    if image is opened in read only mode
                    if image is created with pre set size and trying to write past end of disk
                    if trying to write to part of disk that has already been written to
                    if something went horribly wrong (a bug occured) (should never happen)"""
        inFile = open(filePath, "rb")
        inChunk = inFile.read(self.__bufferSize)
        while inChunk != "":
            self.writeDiskData( self.__writtenDataRawLength + len(self.__incompleteWrittenGrain) , inChunk)
            inChunk = inFile.read(self.__bufferSize)
       
    def writeDiskData(self, offset, data):
        """ Write data to the virtual disk
            args:
                offset: offset in bytes on which to write the data
                data: the data that is be to written
            Throws:
                VMDKStreamException
                    if image not opened yet
                    if image is opened in read only mode
                    if image is created with pre set size and trying to write past end of disk
                    if trying to write to part of disk that has already been written to
                    if something went horribly wrong (a bug occured) (should never happen)"""
        logging.info('image size: %s (mod SECTOR_SIZE: %s), disk size: %s (mod SECTOR_SIZE: %s)'%(self.getImageSize(), self.getImageSize() % SECTOR_SIZE, self.getMaxSize(), self.getMaxSize() % SECTOR_SIZE))
        if not self.__opened:
            raise VMDKStreamException("cannot read: image not opened")
        if self.__readOnly:
            raise VMDKStreamException("cannot write: image is read only")
        if self.__size != -1 and offset + len(data) > self.__size:
            raise VMDKStreamException("cannot write the data: virtual disk too small")    
        if offset < self.__writtenDataRawLength + len(self.__incompleteWrittenGrain):
            raise VMDKStreamException("cannot write the data: data already written to offset")
        if len(data) % SECTOR_SIZE:
            logging.warning("len(data) % SECTOR != 0: padding data to fill up sector")
            data += StreamVmdkMedia.__padToSector(data)
            #Decided not to raise: padding seems more practical to me, but you can uncomment this to be a bit more strict
            #raise VMDKStreamException("data size not integer multiple of SECTOR_SIZE")   
       
        self.__file.seek(0,2)#seek to end of file
        sectorOffset = StreamVmdkMedia.__byteOffsetToSectorOffset(offset)
        grainOffset = StreamVmdkMedia.__sectorOffsetToGrainOffset(sectorOffset) #the grain in which the first byte of data is to be written
        
        if self.__incompleteWrittenGrain != "":#we still have some left over data from a previous write that could not fill up a whole grain
            if grainOffset == self.__writtenDataRawLength / GRAIN_SIZE: #we are going to be writing in the same grain as that left over data
                tempOffset = offset - self.__writtenDataRawLength #get the offset in the unfinished grain
                n = tempOffset - len(self.__incompleteWrittenGrain)
                for i in range(n): #add nulls between the previous piece of data and the new data
                    self.__incompleteWrittenGrain += '\0'
                self.__incompleteWrittenGrain += data[:GRAIN_SIZE - tempOffset] #write the data to fill up this grain
                data = data[GRAIN_SIZE - tempOffset:]
                if len(self.__incompleteWrittenGrain) == GRAIN_SIZE: #if the grain is now full
                    self.__writeData( self.__incompleteWrittenGrain )
                    offset = self.__writtenDataRawLength #we are going to continue writen right after this data
                    sectorOffset = StreamVmdkMedia.__byteOffsetToSectorOffset(offset)
                    grainOffset = StreamVmdkMedia.__sectorOffsetToGrainOffset(sectorOffset)
                else:#if the grain is still now full after writing the data, we must have written all data: return
                    return           
            else:#we are not going to continue writing the same grain as the unfinished data
                for i in range(GRAIN_SIZE - len(self.__incompleteWrittenGrain)):#add padding behind the unfinished data to fill up a grain
                    self.__incompleteWrittenGrain += '\0'
                self.__writeData(self.__incompleteWrittenGrain)
        
        #Here we are guaranteed to have written an integer multiple of GRAIN_SIZE worth of data, and there is no more left over incompeteWrittenData
        initWrittenDataRawLength = self.__writtenDataRawLength
        for i in range(grainOffset - initWrittenDataRawLength / GRAIN_SIZE): #add null blocks between last written data and grain to which we are about to write
            self.__writeEmptyGrain()
        
        ##WRITE TO FIRST GRAIN
        offsetInGrain = offset - grainOffset * GRAIN_SIZE
        for i in range(offsetInGrain):
            self.__incompleteWrittenGrain += '\0'
        self.__incompleteWrittenGrain += data[ : GRAIN_SIZE - offsetInGrain]
        data = data[GRAIN_SIZE - offsetInGrain : ]
        if len(self.__incompleteWrittenGrain) == GRAIN_SIZE:
            self.__writeData(self.__incompleteWrittenGrain)
        else:#if we have not been able to fill up this whole grain, we are done
            return    
        ##We handled the first piece of written data   
        
        ##now we can keep writing whole grains for as long as we need to
        while len(data) >= GRAIN_SIZE:
            currentGrain = data[ : GRAIN_SIZE] #get one GRAIN_SIZE of data
            data = data[GRAIN_SIZE:] #remove the data we are going to write from the data input
            self.__writeData(currentGrain)
            
        
        #keep the left data around to handle on next write / file close
        self.__incompleteWrittenGrain = data
            
    def __writeData(self, data):
        if len(data) != GRAIN_SIZE:
            raise VMDKStreamException("Trying to write data with length != GRAIN_SIZE")
        if data != StreamVmdkMedia.__zeroGrain: 
            self.__writeNonNullGrain(data)
        else:
            self.__writeEmptyGrain()
        
    def __writeNonNullGrain(self, data):
        fileSectorPos = StreamVmdkMedia.__fileToSectorPointer(self.__file)
        
        compressData = zlib.compress(data , self.__compression)
        dataToWrite = StreamVmdkMedia.__createGrainMarker( len( self.__fullGT ) * SECTORS_PER_GRAIN, len(compressData) )
        
        dataToWrite += compressData
        dataToWrite += StreamVmdkMedia.__padToSector(dataToWrite)      
        self.__currentGT.append(fileSectorPos)
        self.__fullGT.append(fileSectorPos)
        self.__file.write(dataToWrite)
        self.__incompleteWrittenGrain = ""
        self.__writtenDataRawLength += GRAIN_SIZE
        if len(self.__currentGT) == self.__parsedHeader.numGTEsPerGT:
            self.__writeGT()
    
    def __writeEmptyGrain(self):
        self.__currentGT.append(0)
        self.__fullGT.append(0)
        self.__incompleteWrittenGrain = ""
        self.__writtenDataRawLength += GRAIN_SIZE
        if len(self.__currentGT) == self.__parsedHeader.numGTEsPerGT:
            self.__writeGT()
        
            
    def __writeGT(self):
        if all(v==0 for v in self.__currentGT):#zero GT: doesn't need to be written, just add 0 to GD
            self.__GD.append(0)
            self.__currentGT = []
            return 
        initLen = len ( self.__currentGT )
        for i in range(self.__parsedHeader.numGTEsPerGT - initLen):
            self.__currentGT.append(0)
            self.__fullGT.append(0)
        marker = StreamVmdkMedia.__createMarker( len(self.__currentGT) / SECTOR_SIZE , MARKER_GT)
        self.__file.write( marker )
        self.__GD.append( StreamVmdkMedia.__fileToSectorPointer(self.__file) )
        dataToWrite = struct.pack("=" + str( len( self.__currentGT )) + "I", *self.__currentGT)
        self.__file.write(dataToWrite)
        self.__currentGT = []
            
    def release(self):
        """dumb impl for now"""
        return self.close()

    def close(self):
        """ closes the file. Writes the GD, footer, all relevant markers and updates the header and descriptor. 
            After closing the file cannot be written to anymore. 
            Throws:
                VMDKStreamException
                    if image is not yet opened"""
        logging.info('pre-close image size: %s (mod SECTOR_SIZE: %s), disk size: %s (mod SECTOR_SIZE: %s)'%(self.getImageSize(), self.getImageSize() % SECTOR_SIZE, self.getMaxSize(), self.getMaxSize() % SECTOR_SIZE))
        if not self.__opened:
            raise VMDKStreamException("cannot close: image not opened")
        if self.__readOnly:
            if not self.__file.closed:
                self.__file.close()
            return
        logging.info("Completing stream VMDK file header..")
        self.__file.seek(0,2)#seek to end of file
        if len(self.__incompleteWrittenGrain) != 0:
            self.__incompleteWrittenGrain += StreamVmdkMedia.__padToGrain(self.__incompleteWrittenGrain)
            self.__writeData(self.__incompleteWrittenGrain)
        descriptor = image_descriptor_template
        descriptor = string.replace(descriptor, "#SECTORS#", str(self.__writtenDataRawLength / SECTOR_SIZE))
        descriptor = string.replace(descriptor, "#CYLINDERS#", str(divro(self.__writtenDataRawLength, (63*255)))) #this formula is mentioned in spec
        descriptor = string.replace(descriptor, "#FILEPATH#", self.__filePath)
        initLen = len(descriptor)
        for i in range( self.__parsedHeader.descriptorSize * SECTOR_SIZE - initLen ):
            descriptor += '\0'
            
        if divro(len(descriptor), SECTOR_SIZE) > self.__parsedHeader.descriptorSize:
            logging.warning("descriptor unexpectedly long (> 3 sectors)")
                    
        if self.__size != -1:
            self.__parsedHeader.capacity = self.__size / SECTOR_SIZE
            self.__parsedFooter.capacity = self.__size / SECTOR_SIZE
            while len(self.__fullGT) * GRAIN_SIZE != self.__size: #fill up the GT with null grains until at correct size
                self.__fullGT.append(0)
                self.__currentGT.append(0)
                if len(self.__currentGT) == self.__parsedHeader.numGTEsPerGT:
                    self.__writeGT() 
        else:
            self.__parsedHeader.capacity = self.__writtenDataRawLength / SECTOR_SIZE
            self.__parsedFooter.capacity = self.__writtenDataRawLength / SECTOR_SIZE
        
        if len(self.__currentGT) != 0:
            self.__writeGT()
        returnPos = self.__file.tell()        
        self.__file.seek(0)
        self.__file.write(self.__parsedHeader.toRawHeader())
        self.__file.write(descriptor)
        self.__file.seek(returnPos)
        
        
        dataToWrite = StreamVmdkMedia.__createMarker( max(1,divro( len(self.__GD) * UINT32_BYTE_SIZE , SECTOR_SIZE)),  MARKER_GD)
        self.__file.write(dataToWrite)
        self.__parsedFooter.gdOffset = StreamVmdkMedia.__fileToSectorPointer( self.__file)
        
        GDSize = max(1,divro( len(self.__GD) * UINT32_BYTE_SIZE , SECTOR_SIZE)) * SECTOR_SIZE / UINT32_BYTE_SIZE
        initGDSize = len(self.__GD)
        for i in range( GDSize - initGDSize):
            self.__GD.append(0)
        
        dataToWrite = struct.pack("=" + str( len( self.__GD )) + "I", *self.__GD)
        dataToWrite += StreamVmdkMedia.__createMarker( 1 ,  MARKER_FOOTER)
        dataToWrite += self.__parsedFooter.toRawHeader()
        dataToWrite += StreamVmdkMedia.__zeroGrain[:SECTOR_SIZE]
        self.__file.write(dataToWrite)
        
        self.__readOnly = True
        self.__file.close()
        logging.info('post-close image size: %s (mod SECTOR_SIZE: %s), disk size: %s (mod SECTOR_SIZE: %s)'%(self.getImageSize(), self.getImageSize() % SECTOR_SIZE, self.getMaxSize(), self.getMaxSize() % SECTOR_SIZE))

    @staticmethod    
    def __createMarker(numSectors, marker_type):
        marker_list = [ numSectors, 0, marker_type ]
        for i in range(496):
            marker_list.append(0)
        marker_struct = "=QII496B"
        return struct.pack(marker_struct, *marker_list)
        
    @staticmethod
    def __fileToSectorPointer(file_object):
        # return file point in sectors
        # raise an exception if not sector aligned
        file_location = file_object.tell()
        if file_location % SECTOR_SIZE:
            raise VMDKStreamException("Asked for a sector pointer on a file whose r/w pointer is not sector aligned")
        else:
            return file_location / SECTOR_SIZE    
    @staticmethod    
    def __createGrainMarker(location, size):
        # The grain marker is special in that the data follows immediately after it
        # without a pad
        return struct.pack("=QI", location, size)    
    @staticmethod
    def __padToSector(data):
        ret = ""
        for i in range(SECTOR_SIZE - len(data) % SECTOR_SIZE):
            ret += '\0'
        return ret
    
    @staticmethod
    def __padToGrain(data):
        ret = ""
        for i in range(GRAIN_SIZE - len(data) % GRAIN_SIZE):
            ret += '\0'
        return ret
            
    @staticmethod
    def __init():
        for i in range(GRAIN_SIZE):
            StreamVmdkMedia.__zeroGrain += '\0'
        StreamVmdkMedia.__initialized = True
    
    #convert from offset in bytes to offset in sectors
    @staticmethod           
    def __byteOffsetToSectorOffset(offset):
        return int( offset / SECTOR_SIZE )      
    
    #convert from offset in sectors to offset in grains
    @staticmethod
    def __sectorOffsetToGrainOffset(offset):
        return int( offset / SECTORS_PER_GRAIN)
    
    
    def __createOpenNew(self):
        """ initialize data structures for a new image """
        self.__file = open(self.__filePath, "wb+")
        self.__readOnly = False
        self.__parsedHeader = ParsedStreamOptimizedHeader()
        self.__parsedFooter = ParsedStreamOptimizedHeader()
        self.__writtenDataRawLength = 0
        self.__compressedGrains = []
        self.__GD = []
        self.__fullGT = [] #all GTs combined, used for easy reading
        self.__currentGT = [] #the current GT we are filling. Will be written to file once full
        self.__incompleteWrittenGrain = ""#this is the last part of a written piece of data, that could not fill up a whole grain
                                          #We save it, so we can append the next data that is being written after it if necacery
        #Allocate space for header and descriptor
        self.__file.write(StreamVmdkMedia.__zeroGrain[:SECTOR_SIZE])
        for i in range(self.__parsedHeader.descriptorSize):
            self.__file.write(StreamVmdkMedia.__zeroGrain[:SECTOR_SIZE])
         
   
    def __openExisting(self):
        """ valid the format of an existing image and initializes data structures to enable reading from it"""
        self.__file = open(self.__filePath,"rb")
        self.__readOnly = True
        fileSize = os.path.getsize(self.__filePath)
        if fileSize % SECTOR_SIZE != 0:
            logging.critical("file size is not a multiple of sector size")
            raise VMDKStreamException("File is of wrong format or corrupted")
        if fileSize < SECTOR_SIZE * 4:
            logging.critical("file size too small: cannot be a valid image")
            raise VMDKStreamException("File is of wrong format or corrupted")
        
        rawHeader = self.__file.read(SECTOR_SIZE)
        try:
            self.__parsedHeader = ParsedStreamOptimizedHeader(rawHeader)
        except VMDKStreamException as e:
            raise VMDKStreamException("File is of wrong format or corrupted")
        
        
        self.__file.seek(fileSize-SECTOR_SIZE*3)
        footerMarker = self.__file.read(SECTOR_SIZE)
        parsedFooterMarker = struct.unpack("=QII496B",footerMarker)
        if parsedFooterMarker[0] != 1 or \
           parsedFooterMarker[1] != 0 or \
           parsedFooterMarker[2] != MARKER_FOOTER or \
           not all(v == 0 for v in parsedFooterMarker[3:499]):
           logging.critical("incorrect footer marker")
           raise VMDKStreamException("File is of wrong format or corrupted")
           
        
        rawFooter = self.__file.read(SECTOR_SIZE)
        try:
            self.__parsedFooter = ParsedStreamOptimizedHeader(rawFooter)
        except VMDKStreamException as e:
            raise VMDKStreamException("File is of wrong format or corrupted")
        
        if not ParsedStreamOptimizedHeader.validHeaderFooterPair(self.__parsedHeader, self.__parsedFooter):
            logging.critical("non valid header/footer pair")
            raise VMDKStreamException("File is of wrong format or corrupted")
            
        EOSMarker = self.__file.read(SECTOR_SIZE)
        if EOSMarker != self.__zeroGrain[:SECTOR_SIZE]:
            logging.critical("Incorrect EOS marker")
            raise VMDKStreamException("File is of wrong format or corrupted")
        
        
        GDFileLocation = self.__parsedFooter.gdOffset * SECTOR_SIZE
        totalGrains = self.__parsedHeader.capacity / SECTORS_PER_GRAIN#total amount of grains in the virtual disk
        totalGTs = divro(totalGrains, self.__parsedHeader.numGTEsPerGT)#total amount of GTs needed for that many grains
        GDSectors = divro(totalGTs, SECTOR_SIZE/ UINT32_BYTE_SIZE)#total number of sectors needed for that many GTs
        
        self.__file.seek(GDFileLocation)
        rawGD = self.__file.read(GDSectors*SECTOR_SIZE) #read the raw GD
        self.__GD = struct.unpack("="+str(len(rawGD) / UINT32_BYTE_SIZE)+"I", rawGD)[:totalGTs] #store the unpacked GD

        self.__fullGT = []#This will be the full parsed GT (all tables combined)
        for i in range(totalGTs):
            if self.__GD[i] != 0: # A zero entry in the GD indicates there is no backing GT
                self.__file.seek(self.__GD[i] * SECTOR_SIZE) #Go to the pointer at location
                GT = self.__file.read(self.__parsedHeader.numGTEsPerGT * UINT32_BYTE_SIZE) #read the GT
                self.__fullGT += struct.unpack("="+str( self.__parsedHeader.numGTEsPerGT )+"I", GT) #and add the parsed entries
            else:
                for i in range(self.__parsedHeader.numGTEsPerGT):
                    self.__fullGT.append(0)
        self.__fullGT = self.__fullGT[:totalGrains]

