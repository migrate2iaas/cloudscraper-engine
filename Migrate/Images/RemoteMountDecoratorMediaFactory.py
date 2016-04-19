"""

Class representing RemoteMountDecoratorMediaFactory class

"""

import ImageMediaFactory
import ImageMedia
import os;
import time

class RemoteMountDecoratorMediaFactory(ImageMediaFactory.ImageMediaFactory):
    """
    this factory class is decorator class for actual media factory

    it decorates existing media factory with an external mount subdirectory

    So if images are going to be created like /xxx/xxx/disk.img they will be created like /xxx/xxx/mountpoint/disk.img
    """

    def __init__(self , mount_obj, actual_factory):
        """
        Args:
            mount_obj - any object that supports mount(direcory) operation , e.g. fuse
            actual_factory - factory to be decorated
        """
        self.__mounter = mount_obj
        self.__factory = actual_factory
        self.__mountdir = ""
        return

    def createMedia(self , imagepath , imagesize):
        if self.__mounter.isMounted() == False:
            basedir = os.path.dirname(imagepath);
            self.__mountdir = os.path.join( basedir, "mount" + str(int(time.time())))
            self.__mounter.mount(self.__mountdir)
            #TODO: think when to do unmount
        
        mountedpath = self.__mountdir + os.path.basename(imagepath)
        
        return self.__factory(mountedpath , imagesize)


