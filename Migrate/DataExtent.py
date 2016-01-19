# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging

class DataExtent(object):
    """one linear data interval"""
    # members:
   # __startInBytes = long(0)
   # __sizeInBytes = long(0)
    def __init__(self, start , size):
        self.__startInBytes = start
        self.__sizeInBytes = size
        self.__data = None
        #make it pseudo iterable
        self.__iterated = None

    def getSize(self):
        return self.__sizeInBytes
    def getStart(self):
        return self.__startInBytes
    
    def getData(self):
        """gets data as byte array"""
        return str(self.__data)
    
    def setData(self, data):
        """ the subsequental data read might be defered, it's stringizing (__str__) method should read the data"""
        self.__data = data

    def __iter__(self):
        self.__iterated = True
        return self

    def next(self):
        if self.__iterated:
            self.__iterated = None
            return self
        raise StopIteration

    def __getitem__(self, index):
        if index == 0:
            return self
        raise IndexError

    def __str__(self):
        return "["+str(self.__startInBytes)+";"+str(self.__startInBytes+self.__sizeInBytes)+")"

    def __contains__(self,other):
        if self.getStart() <= other.getStart() and other.getStart() + other.getSize() <= self.getSize() + self.getStart():
            return True
        return False
    
    def __cmp__(self, other):
        if self.getStart() > other.getStart() and self.getStart() >= other.getStart() + other.getSize():
            return 1
        if other.getStart() > self.getStart() and other.getStart() >= self.getStart() + self.getSize():
            return -1
        if other.getStart() == self.getStart() and other.getSize() == self.getSize():
            return 0
        return NotImplemented 
   
    def intersect(self, other):
         if self.getStart() <= other.getStart() and other.getStart() <= self.getSize() + self.getStart():
            return True
         if other.getStart() < self.getStart() and other.getStart() +  other.getSize() > self.getStart():
            return True
         return False 
    
    # returns block describing the intersection
    def intersection(self, other):
        if self.intersect(other) == False:
            return None

        #TODO: calc the intersection between two intervals
        #if other.getStart() - self.getStart() > 0:
        #    pieces.append(SplittedDataExtent(other.getStart(), self.getStart() - self.getStart() , self) )
        #if other.getStart() + other.getSize() < self.getStart() + self.getSize():
        #    pieces.append(SplittedDataExtent( other.getStart() + other.getSize() , self.getStart() + self.getSize() - (other.getStart() + other.getSize()) , self ) )
        # V other        | self
        #
        # V-----|+++++++V++++++|
        # V-----|+++++++|------V
        # |+++++V-------|------V
        if other.getStart() > self.getStart():
            start = other.getStart()
        else:
            start = self.getStart()
        if  other.getStart() + other.getSize() > self.getStart() + self.getSize():
            end = self.getStart() + self.getSize()
        else:
            end = other.getStart() + other.getSize()

        if end - start > 0:
            return SplittedDataExtent(start, end-start, self)
        else:
            return None

         
    # returns list of pieces left after substraction the intersection. The original extent is not affected     
    def substract(self, other):
        pieces = list()
        if self.intersect(other) == False:
            return pieces
        
        if other.getStart() - self.getStart() > 0:
            pieces.append(SplittedDataExtent(self.getStart(), other.getStart() - self.getStart() , self) )
        if other.getStart() + other.getSize() < self.getStart() + self.getSize():
            pieces.append(SplittedDataExtent( other.getStart() + other.getSize() , self.getStart() + self.getSize() - (other.getStart() + other.getSize()) , self ) )
        return pieces
    
    # split onto two pieces starting from the splitstart returns tuple ( [start;splitstart);[splitstart;start+size) )
    def split(self, splitstart):
        if (splitstart < self.__startInBytes or splitstart > self.__sizeInBytes + self.__startInBytes):
            return
        pieces = self.substract(DataExtent(splitstart, 0))
        return (pieces[0], pieces[1])
        

class CachedDataExtent(DataExtent):
    """the extent that caches data that is read so re-read of same extent comes in no cost"""
    
    def __init__(self, start , size):
        """constructor"""
        super(CachedDataExtent, self).__init__(start, size)
        self.__cachedData = None

    def getData(self):
        """override that checks if data was already read"""
        if self.__cachedData:
            return self.__cachedData
        data = super(CachedDataExtent, self).getData()
        if data:
            self.__cachedData = data
        return data


class SplittedDataExtent(DataExtent):
    """the part of one original extent"""

    def __init__(self, start, size, originalext):
        self.__originalExt = originalext
        super(SplittedDataExtent, self).__init__(start, size)
         
    def getData(self):
        #NOTE: means someone has changed the data by setData call
        if self._DataExtent__data:
            logging.debug("SplittedDataExtent: Getting data from base class data.")  
            return super(SplittedDataExtent, self).getData()
        data = self.__originalExt.getData()
        return data[self.getStart() -  self.__originalExt.getStart():self.getStart() - self.__originalExt.getStart() + self.getSize()]
