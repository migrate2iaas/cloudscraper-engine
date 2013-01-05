

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
        return str(self.__data)
    # the data might be defered, it's stringizing (__str__) method should read the data
    def setData(self, data):
        self.__data = data

    def __iter__(self):
        self.__iterated = True
        return self

    def next(self):
        if self.__iterated:
            self.__iterated = None
            return self
        raise StopIteration

    def __str__(self):
        return "["+str(self.__startInBytes)+str(self.__startInBytes+self.__sizeInBytes)+")"

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
         return False;
        
    # returns list of pieces left after substraction the intersection. The original extent is not affected     
    def substract(self, other):
        pieces = list()
        if self.intersect(other) == False:
            return pieces
        
        if other.getStart() - self.getStart() > 0:
            pieces.append(DataExtent(self.getStart(), other.getStart() - self.getStart() ) )
        if other.getStart() + other.getSize() < self.getStart() + self.getSize():
            pieces.append(DataExtent( other.getStart() + other.getSize() , self.getStart() + self.getSize() - (other.getStart() + other.getSize()) ) )
        return pieces
            
        
         
