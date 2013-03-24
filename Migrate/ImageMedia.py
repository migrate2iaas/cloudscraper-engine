# the base class for all the media to contain a system or data image

class ImageMedia(object):
    """Base class to represent media to store an image"""

    #starts the connection
    def open(self):
        raise NotImplementedError

    def getMaxSize(self):
        raise NotImplementedError

    def reopen(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError
    
    def release(self):
        raise NotImplementedError

    #sets the channel so the data may be sent whenever data changes
    def setChannel(self):
        raise NotImplementedError