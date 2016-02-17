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
import md5

#TODO: move to configs
debug_replacepent = True

#NOTE: defred read could also be used when filesize is too large to keep in memory
class ReplacedData(object):
    """Reader for replaced data, needed for debug purposes"""
    def __init__(self, data, size, filename, fileoffset):
        self.__data = str(data)
        self.__size = size
        self.__filename = filename
        self.__fileoffset = fileoffset
        logging.debug("\t Seting replaced data for file " + self.__filename + " at offset " + str(self.__fileoffset) + " of size " + str(self.__size))

        if debug_replacepent:
            md5encoder = md5.md5()
            md5encoder.update(self.__data)
            logging.debug("\t Md5 = " + str(md5encoder.hexdigest()))

    def __str__(self):
        logging.debug("\t Getting replaced data for file " + self.__filename + " at offset " + str(self.__fileoffset) + " of size " + str(self.__size))
        if debug_replacepent:
            md5encoder = md5.md5()
            md5encoder.update(self.__data)
            logging.debug("\t Md5 = " + str(md5encoder.hexdigest()))
        return self.__data

#for nner use only
# it uses simple file io to read files
class AdjustedFileToBackup(FileToBackup.FileToBackup):
     """ for Adjusted file enumerator"""
    
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

        self.__file.seek(volextent.getStart()) 
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
        """constructor"""
        self.__fileIterator = fileIterator
        self.__adjustOption = adjustOption
        self.__backupSource = originalBackupSource
        self.__addedFiles = self.__adjustOption.getAddedFilesEnum()
    
    def __iter__(self):
        """makes it iterable"""
        return self

    def next(self):
        """returns name of the file"""

        if self.__addedFiles != None:
            try:
               (destfilename, sourcefilename) = self.__addedFiles.next()
               return AdjustedFileToBackup(sourcefilename, destfilename, self.__backupSource)
               
            except StopIteration:
                self.__addedFiles = None

        file = self.__fileIterator.next()
        while self.__adjustOption.isFileRemoved(file.getName()):
            logging.debug("Skipping file " + str(file.getName())) 
            file = self.__fileIterator.next()
        
        return file
        


class AdjustedBackupSource(BackupSource.BackupSource):
    """
    Adjusted backup source. Thea adjusts are: file removal, renaming, replacing contents
    Impements BackupSource interface with all the adjusts needed. 
    It bridges (pointer to impl) the backup source
    """

    def __init__ (self):
        """default constructor"""
        __adjustOption = None
        __backupSource = None
        super(AdjustedBackupSource,self).__init__() 

    def setAdjustOption(self,adjustOption):
        """sets the adjust options: what files and how to adjust. Adjust option filters the output of file blocks returned by aapropraite functions"""
        self.__adjustOption = adjustOption

    def getAdjustOption(self):
        """gets adjust option: what and which files to adjust"""
        return self.__adjustOption

    def setBackupSource(self,backupSource):
        """sets the associated backup source, the place where data is got"""
        self.__backupSource = backupSource

    def getBackupSource(self):
        """gets the associated backup source"""
        return self.__backupSource

    #Overrides:

    def getFileEnum(self , root="\\", mask="*"):
        """
        Gets files enumerator(iterable)
        
        Args:
            root - Directory root where to start the search
            mask - mask to search (* - is any)

        Return ietrable AdjustedFileEnum
        """
        if self.__backupSource == None:
            raise PropertyNotInitialized("__backupSource", " Use setBackupSource() to init it")
        if self.__adjustOption == None:
            raise PropertyNotInitialized("__adjustOption", " Use setAdjustOption() to init it")
        return AdjustedFileEnum(self.__adjustOption, self.__backupSource.getFileEnum(root, mask), self.__backupSource)

    def getFilesBlockRange(self):
        """
        Gets block ranges occupied by files 

        Return the iterable of DataExtent objects
        """
        if self.__backupSource == None:
            raise PropertyNotInitialized("__backupSource", " Use setBackupSource() to init it")
        if self.__adjustOption == None:
            raise PropertyNotInitialized("__adjustOption", " Use setAdjustOption() to init it")
        blocksrange = self.__backupSource.getFilesBlockRange()

        # Calculate data size
        existing_size = removed_size = 0
        for extent in blocksrange:
            existing_size += extent.getSize()
        logging.info("Overall data size {0}MB".format(existing_size / (1024 * 1024)))

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
                logging.debug("Removed extent {0}".format(removedextent))
                for block in blocksrange[:]:

                    if removedextent.intersect(block):
                        blockindex = blocksrange.index(block)
                        blocksrange.remove(block)
                        removed_size += block.getSize()
                        logging.debug("Removing block {0}".format(block))
                        pieces = block.substract(removedextent)
                        for piece in pieces:
                            blocksrange.insert(blockindex+pieces.index(piece), piece)
                            removed_size -= piece.getSize()
                            logging.debug("Inserting piece {0}".format(piece))

        blocksrange = sorted(blocksrange)
        logging.info("Excluded data size {0}MB".format(removed_size / (1024 * 1024)))
        #adding replaced files
        for (removed, replacement) in self.__adjustOption.getReplacedFilesEnum():
            replacedfile = removed
            volname = self.__backupSource.getBackupDataSource().getVolumeName()
            if replacedfile.startswith(volname):
                #remove the starting volume path if any
                replacedfile = replacedfile.replace(volname + "\\" , "" , 1)
            try:
                replacedrange = self.__backupSource.getFileBlockRange(replacedfile)
            except Exception as ex:
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
                        logging.debug("\tFound intersection of volume block " + str(block) + " with " + str(replacedext) + " = " + str(replacedpart)) 
                        blockindex = blocksrange.index(block)
                        blockstoremove.append(block)
                        pieces = block.substract(replacedext)
                        #TODO: checks and logging here

                        logging.debug("\tChanging block " + str(replacedpart) + " by data from offset " + str(replacedoffset) + " of replacement file " + str(replacement)) 
                        filereplacement = open(replacement, "rb")
                        
                        filereplacement.seek(replacedoffset)
                        data = filereplacement.read(replacedpart.getSize())
                        if (len(data) != replacedpart.getSize()):
                            logging.warning("Cannot get enough data from the replacement "+  replacement + " of file " + replacedfile + ": " + str(len(data)) + " instead of " + str(replacedpart.getSize())   )
                        replaceddata = ReplacedData(data , len(data) , removed , replacedoffset)
                        replacedpart.setData(replaceddata) 

                        if debug_replacepent:
                            debug_copy_name = replacement+"copy"
                            if os.path.exists(debug_copy_name):
                                filereplacement_copy = open(debug_copy_name, "r+b")
                            else:
                                filereplacement_copy = open(debug_copy_name, "w+b")
                            filereplacement_copy.seek(replacedoffset)
                            filereplacement_copy.write(data)

                            md5encoder = md5.md5()
                            md5encoder.update(data)
                            logging.debug("\t Data copy Md5 = " + str(md5encoder.hexdigest()))

                            filereplacement_copy.close()

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
                    logging.debug("\tAdding block " + str(block) +" to overall block list")
                blocksrange.extend(newblocks)
                blocksrange = sorted(blocksrange)

        logging.info("Resulting data size {0}MB".format((existing_size - removed_size) / (1024 * 1024)))
        return sorted(blocksrange)
                       
        
    def getFileBlockRange(self, filename):
        """
        Gets block range for file specified

        Return iterable of DataExtent objects
        """
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

    def replacementSelfCheck(self, mediapath):
        """
        Compares if the replaced data on resulting FS (idenitfied by meidapath) is equal to data in replacing files.
        Used for self-testing purposes
        """
        for (replacedfile,replacement) in self.__adjustOption.getReplacedFilesEnum():
             volname = self.__backupSource.getBackupDataSource().getVolumeName()
             if replacedfile.startswith(volname):
                 #remove the starting volume path if any
                 replacedfile = replacedfile.replace(volname + "\\" , "" , 1)
             
             replacedoffset = 0
             size = 4096*4096 
             offset = 0

             filereplacement = open(replacement, "rb")
             targetpath = mediapath+"\\"+replacedfile
             filetarget = open(targetpath , "rb") 

             while 1:
                datasrc = filereplacement.read(size)
                datatarget = filetarget.read(size)
                if len(datasrc) == len(datatarget):
                    if datasrc == datatarget:
                        offset += len(datasrc)
                        continue
                    else:
                        logging.error("!!!Error: image corruption detected during the self-check!")
                        logging.error("Data at offset " + str(offset) + " is not equal for file " + replacedfile) 
                        break
                else:
                    logging.error("!!!Error: image corruption detected during the self-check!")
                    logging.error("Data returned size at offset " + str(offset) + " is dfifferent for two files " + replacedfile) 
                    break
                if len(datasrc) == 0:
                    break

             filereplacement.close()
             filetarget.close()

