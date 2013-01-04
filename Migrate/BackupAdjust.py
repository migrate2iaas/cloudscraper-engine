class BackupAdjust(object):
    """Adjusts the backup source"""

    def __init__(self):
        self.__extraFiles = dict()
        self.__removedFiles = set()
        return

    # adds source file (filename or file handle) to dest file path
    def addFile(self, sourceFile, destFileName):
        self.__extraFiles[destFileName] = sourceFile
        return

    # removes file by its name
    def removeFile(self , sourceFile):
        self.__removedFiles.add(sourceFile)
        return

    # checks if the file removed
    def isFileRemoved(self , sourceFile):
        return sourceFile in self.__removedFiles

    # tuples (destfile, sourcefile)
    def getAddedFilesEnum(self):
        return self.__extraFiles.iteritems()

     # sourcefiles iterable
    def getRemovedFilesEnum(self):
        return self.__removedFiles.__iter__()