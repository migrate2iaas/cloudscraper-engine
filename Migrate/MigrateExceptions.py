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
         return repr("Class property " + self.__valuename + " not initialized. " + self.__text)
    def __repr__(self):
        return repr("<Class property " + self.__valuename + " not initialized. " + self.__text + " >")


class FileException(Exception):
    def __init__(self, filename, exception):
       self.__filename = filename
       self.__exception = exception

       # Saving error code number
       self.errorno = exception[0]

    def __str__(self):
         return "Exception on file \'" + str(self.__filename) + "\' : " + str(self.__exception)


class AccessDeniedException(FileException):
    def __init__(self, filename):
        self.__filename = filename

    def __str__(self):
        return "Access denied exception on {0}".format(self.__filename)

