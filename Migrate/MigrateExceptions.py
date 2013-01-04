
#this file define some auxillary exceptions

class PropertyNotInitialized(Exception):
    def __init__(self, valuename, text):
       self.__valuename = valuename
       self.__text = text
    def __str__(self):
         return "Class propeerty " + self.valuename + " not initialized. " + text;
