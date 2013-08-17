# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import FileToBackup
import BackupAdjust
import BackupSource
from MigrateExceptions import *
import logging
import difflib
import DataExtent
import os
import sys

#NOTE: defred read could also be used when filesize is too large to keep in memory
class ReplacedData(object):
    """Reader for replaced data, needed for debug purposes"""
    def __init__(self, data, size, filename, fileoffset):
        self.__data = data
        self.__size = size
        self.__filename = filename
        self.__fileoffset = fileoffset
        logging.debug("\t Seting replaced data for file " + self.__filename + " at offset " + str(self.__fileoffset) + " of size " + str(self.__size))
    def __str__(self):
        logging.debug("\t Getting replaced data for file " + self.__filename + " at offset " + str(self.__fileoffset) + " of size " + str(self.__size))
        return self.__data

#for nner use only
# it uses simple file io to read files
class AdjustedFileToBackup(FileToBackup.FileToBackup):
     """Adjusted file enumerator"""
    
     def getName(self):
        return self.__name


     def __init__(self, srcPath, destPath, backupSource):
        self.__name = srcPath
        self.__srcPath = srcPath
        self.__backupSource = backupSource
        
        self.__destPath = destPath
        self.__transferDest = None

        self.__file = None
    
     def getDestPath(self):
        return self.__destPath

     def getSourcePath(self):
        return self.__srcPath 

     def getBackupSource(self):
        return self.__backupSource

     def getTransferDest(self):
        return self.__transferDest

     def getChangedExtents(self):
        self.__reopen()
        size = os.stat(self.getSourcePath()).st_size
        return DataExtent.DataExtent(0 , size)

     #returns data read
     def readData(self,volextent):
        self.__reopen()

        self.__file.seek(volextent.getStart());
        output = self.__file.read(volextent.getSize())

        self.__close()
        return output
    
     #reopens source file if needed
     def __reopen(self):
        if self.__file == None:
            self.__file = open( self.getSourcePath(), mode = "rb" )
    
     def __close(self):
        self.__file.close()

    
#for inner use only
class AdjustedFileEnum(object):
    """Adjusted file enumerator: adjusts original enumeration removing or adding special files to the backup list"""
    
    def __init__(self, adjustOption , fileIterator, originalBackupSource):
        self.__fileIterator = fileIterator
        self.__adjustOption = adjustOption
        self.__backupSource = originalBackupSource
        self.__addedFiles = self.__adjustOption.getAddedFilesEnum()
    
    def __iter__(self):
        return self

    #returns name of the file
    def next(self):

        if self.__addedFiles != None:
            try:
               (destfilename, sourcefilename) = self.__addedFiles.next()
               return AdjustedFileToBackup(sourcefilename, destfilename, self.__backupSource)
               
            except StopIteration:
                self.__addedFiles = None

        file = self.__fileIterator.next()
        while self.__adjustOption.isFileRemoved(file.getName()):
            file = self.__fileIterator.next()
        
        return file
        


class AdjustedBackupSource(BackupSource.BackupSource):
    """Adjusted backup source. Impements BackupSource interface with all the adjusts needed. It bridges (pointer to impl) the backup source"""

    def __init__ (self):
        __adjustOption = None
        __backupSource = None

    def setAdjustOption(self,adjustOption):
        self.__adjustOption = adjustOption

    def getAdjustOption(self):
        return self.__adjustOption

    def setBackupSource(self,backupSource):
        self.__backupSource = backupSource

    def getBackupSource(self):
        return self.__backupSource

    #Overrides:

    # gets files enumerator
    def getFileEnum(self , root="\\", mask="*"):
        if self.__backupSource == None:
            raise PropertyNotInitialized("__backupSource", " Use setBackupSource() to init it")
        if self.__adjustOption == None:
            raise PropertyNotInitialized("__adjustOption", " Use setAdjustOption() to init it")
        return AdjustedFileEnum(self.__adjustOption, self.__backupSource.getFileEnum(root, mask), self.__backupSource)

    # gets block range for range of files
    def getFilesBlockRange(self):
        if self.__backupSource == None:
            raise PropertyNotInitialized("__backupSource", " Use setBackupSource() to init it")
        if self.__adjustOption == None:
            raise PropertyNotInitialized("__adjustOption", " Use setAdjustOption() to init it")
        blocksrange = self.__backupSource.getFilesBlockRange()

        #removing removed files from the filled extents
        for removed in self.__adjustOption.getRemovedFilesEnum():
            removedfile = removed
            volname = self.__backupSource.getBackupDataSource().getVolumeName()
            if removedfile.startswith(volname):
                removedfile = removedfile.replace(volname + "\\" , "" , 1)
            try:
                removedrange = self.__backupSource.getFileBlockRange(removedfile)
            except Exception as ex:
                logging.warning("Cannot get blocks for removed file " + removedfile)
                logging.debug("Reason: " + str(ex))
                continue
            #NOTE: it may work faster if it's sorted, ugly code by the way
            for removedextent in removedrange:
                for block in blocksrange:
                    if removedextent.intersect(block):
                        blockindex = blocksrange.index(block)
                        blocksrange.remove(block)
                        pieces = block.substract(removedextent)
                        for piece in pieces:
                             blocksrange.insert(blockindex+pieces.index(piece), piece)
                        break

        blocksrange = sorted(blocksrange)
        #adding replaced files
        for (removed,replacement) in self.__adjustOption.getReplacedFilesEnum():
            replacedfile = removed
            volname = self.__backupSource.getBackupDataSource().getVolumeName()
            if replacedfile.startswith(volname):
                #remove the starting volume path if any
                replacedfile = replacedfile.replace(volname + "\\" , "" , 1)
            try:
                replacedrange = self.__backupSource.getFileBlockRange(replacedfile)
            except:
                logging.warning("Cannot get blocks for replaced file " + replacedfile)
                logging.debug("Reason: " + str(ex))
                continue
            replacedoffset = 0
            # iterate thru all extents in file to replace, see what blocks it intersects with
            for replacedext in replacedrange:
                newblocks = list()
                blockstoremove = list()
                #SOMEHOW THE BLOCK IS DIVIDED BUT NOT FILLED IN THE APPROPRIATE FASHION
                #ASSUME blocksrange is not sorted and we get errors
                for block in blocksrange:
                    # we devide the big block of filled data onto three parts: after,before replaced file extent, and the extent itself
                    replacedpart = replacedext.intersection(block)
                    if replacedpart:
                        logging.debug("\tFound intersection of volume block " + str(block) + " with " + str(replacedext) + " = " + str(replacedpart));
                        blockindex = blocksrange.index(block)
                        blockstoremove.append(block)
                        pieces = block.substract(replacedext)
                        #TODO: checks and logging here

                        logging.debug("Changing block " + str(replacedpart) + " by data from offset " + str(replacedoffset) + " of replacement file " + str(replacement));
                        filereplacement = open(replacement, "rb")
                        
                        filereplacement.seek(replacedoffset)
                        data = filereplacement.read(replacedpart.getSize())
                        if (len(data) != replacedpart.getSize()):
                            logging.warning("Cannot get enough data from the replaced file: " + str(len(data)) + " instead of " + str(replacedpart.getSize()) )
                        replaceddata = ReplacedData(data , len(data) , removed , replacedoffset)
                        replacedpart.setData(replaceddata) 

                        replacedoffset = replacedoffset + len(data)
                        filereplacement.close()

                        pieces.append(replacedpart)
                        # TODO: unittest for data extent with more than stupids asserts
                        # add new blocks to the 
                        newblocks.extend(pieces)

                        #break - there could be lots of neighboor blocks cause blocks are divided into parts. see WindowsVolume.py line 264
                for block in blockstoremove:
                    logging.debug("\tRemoving inital block " + str(block) + " from block list")
                    blocksrange.remove(block)
                for block in newblocks:
                    logging.debug("\tAdding block "+ str(block) +" to overall block list")
                blocksrange.extend(newblocks)
                blocksrange = sorted(blocksrange)

        return sorted(blocksrange)
                       
        

    # gets block range for file specified
    def getFileBlockRange(self, filename):
        if self.__backupSource == None:
            raise PropertyNotInitialized("__backupSource", " Use setBackupSource() to init it")
        if self.__adjustOption == None:
            raise PropertyNotInitialized("__adjustOption", " Use setAdjustOption() to init it")
        
        if self.__adjustOption.isFileRemoved(filename):
            return list()
        if self.__adjustOption.isFileReplaced(filename):
            replacement = self.__adjustOption.fileReplacement(filename)
            replacedrange = self.__backupSource.getFileBlockRange(filename)
            replacedoffset = 0
            for replacedext in replacedrange:
                filereplacement = open(replacement, "rb")
                filereplacement.seek(replacedoffset)
                replacedext.setData(filereplacement.read(replacedext.getSize())) 
                replacedoffset = replacedoffset + replacedext.getSize()
                filereplacement.close()
            return replacedrange

        return self.__backupSource.getFileBlockRange(filename)

    #compares if the replaced data on resulting FS (idenitfied by meidapath) is equal to data in replacing files.
    #Used for self-testing purposes
    def replacementSelfCheck(self, mediapath):
        for (replacedfile,replacement) in self.__adjustOption.getReplacedFilesEnum():
             volname = self.__backupSource.getBackupDataSource().getVolumeName()
             if replacedfile.startswith(volname):
                 #remove the starting volume path if any
                 replacedfile = replacedfile.replace(volname + "\\" , "" , 1)
             
             replacedoffset = 0
             size = 4096*4096;
             offset = 0

             filereplacement = open(replacement, "rb")
             targetpath = mediapath+"\\"+replacedfile
             filetarget = open(targetpath , "rb");

             while 1:
                datasrc = filereplacement.read(size)
                datatarget = filetarget.read(size)
                if len(datasrc) == len(datatarget):
                    if datasrc == datatarget:
                        offset += len(datasrc)
                        continue
                    else:
                        logging.error("!!!Error: image corruption detected during the self-check!")
                        logging.error("Data at offset " + str(offset) + " is not equal for file " + replacedfile);
                        break
                else:
                    logging.error("!!!Error: image corruption detected during the self-check!")
                    logging.error("Data returned size at offset " + str(offset) + " is dfifferent for two files " + replacedfile);
                    break
                if len(datasrc) == 0:
                    break

             filereplacement.close()
             filetarget.close()

