
class AdjustedBackupSource(BackupSource):
    """Adjusted backup source. Impements BackupSource interface with all the adjusts needed. It bridges (pointer to impl) the backup source"""

    __adjustOption = null
    __backupSource = null

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
        return AdjustedFileEnum(self.__adjustOption, __backupSource.getFileEnum())

    # gets block range for range of files
    # backupOptions - options incl file excludes, etc
    def getFilesBlockRange(self, backupOptions):
        blocksrange = __backupSource.getFilesBlockRange(backupOptions)
        #here we filter out the deleted elements
        blocksrange 


