# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import FileToBackup
import BackupAdjust
import BackupSource
import MigrateExceptions
import logging

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
        size = GetFileSize(self.__hFile)
        return DataExtent(0 , size)

     #returns data read
     def readData(self,extent):
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
        if self.__hFile != None:
            self.__file.close()
            self.__hFile = None

    
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
    def getFileEnum(self):
        if self.__backupSource == None:
            raise PropertyNotInitialized("__backupSource", " Use setBackupSource() to init it")
        if self.__adjustOption == None:
            raise PropertyNotInitialized("__adjustOption", " Use setAdjustOption() to init it")
        return AdjustedFileEnum(self.__adjustOption, self.__backupSource.getFileEnum(), self.__backupSource)

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
            except:
                logging.warning("Cannot get blocks for removed file " + removedfile)
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

        #adding replaced files
        for (removed,replacement) in self.__adjustOption.getReplacedFilesEnum():
            replacedfile = removed
            volname = self.__backupSource.getBackupDataSource().getVolumeName()
            if replacedfile.startswith(volname):
                replacedfile = replacedfile.replace(volname + "\\" , "" , 1)
            try:
                replacedrange = self.__backupSource.getFileBlockRange(replacedfile)
            except:
                logging.warning("Cannot get blocks for removed file " + replacedfile)
                continue
            replacedoffset = 0
            for replacedext in replacedrange:
                for block in blocksrange:
                    if replacedext.intersect(block):
                        blockindex = blocksrange.index(block)
                        blocksrange.remove(block)
                        pieces = block.substract(replacedext)
                        #TODO: checks and loggig here

                        filereplacement = open(replacement, "rb")
                        filereplacement.seek(replacedoffset)
                        replacedext.setData(filereplacement.read(replacedext.getSize())) 
                        replacedoffset = replacedoffset + replacedext.getSize()
                        filereplacement.close()

                        pieces.append(replacedext)
                        # TODO: unittest for data extent with more than stupids asserts
                        pieces = sorted(pieces)
                        for piece in pieces:
                            blocksrange.insert(blockindex+pieces.index(piece), piece)
                        break

        return blocksrange
                       
        

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


