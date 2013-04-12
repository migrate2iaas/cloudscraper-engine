# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

#this file define some auxillary exceptions

class PropertyNotInitialized(Exception):
    def __init__(self, valuename, text):
       self.__valuename = valuename
       self.__text = text
    def __str__(self):
         return repr("Class property " + self.valuename + " not initialized. " + text)
    def __repr__(self):
        return repr("<Class property " + self.valuename + " not initialized. " + text + " >")



    
class FileException(Exception):
    def __init__(self, filename, exception):
       self.__filename = filename
       self.__exception = exception
    def __str__(self):
         return "Exception on file \'" + str(self.__filename) + "\' : " + str(self.__exception)