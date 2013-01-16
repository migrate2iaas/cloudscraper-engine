import random
import SystemAdjustOptions
    

class WindowsSystemAdjustOptions(SystemAdjustOptions.SystemAdjustOptions):    
     """special Windows options"""
     
     def __init__(self):
         random.seed()
         self.__systemPartStart = long(0x0100000);
         self.__newMbr = int(random.randint(1, 0x0FFFFFFF))
         self.__prebuiltBcdPath = "..\\resources\\boot\\win\\BCD_MBR"

     # override loading the data from config file
     def loadConfig(self , adjustOptionConfig):
         return 

     
     #Windows special methods

     #
     def getPregeneratedBcdHivePath(self):
        return self.__prebuiltBcdPath

     #
     def getNewMbrId(self):
        return self.__newMbr
    
     #
     def setNewMbrId(self , newMbr):
        self.__newMbr = newMbr 

     #  
     def getNewSysPartStart(self):
        return self.__systemPartStart
    
     #
     def setNewSysPartStart(self , newPart):
         self.__systemPartStart = newPart

     # gets new mount points
     def getMountPoints():
         return