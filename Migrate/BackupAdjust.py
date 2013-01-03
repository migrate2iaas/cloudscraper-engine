class BackupAdjust(object):
    """Adjusts the backup source"""

    __extraFiles = null
    __removedFiles = null

    def __init__():
        __extraFiles = dict()
        __removedFiles = set()
        return

    # adds source file (filename or file handle) to dest file path
    def addFile(sourceFile, destFileName):
        __extraFiles[destFileName] = sourceFile
        return

    # removes file by its name
    def removeFile(sourceFile):
        __removedFiles.add(sourceFile)
        return

