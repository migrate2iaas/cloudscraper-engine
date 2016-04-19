"""

This file describes FUSE Wrapper class

The intention is create store data in a FUSE-based filesystem

"""


class FuseWrapper(object):
    """Fuse wrapper is a wrapper interface to create images inside Fuse-based filesystem"""


    def __init__(self):
        self.__mountDir = ""
        self.__fuseObj = None

    def mount(self, directory):
        self.__mountDir = directory 
        fuse = FUSE(fuse_obj, self.__mountDir, foreground=False)

    def unmount(self):
        pass

    def isMounted(self):
        return self.__mountDir != ""

    def getMountDir(self):
        return self.__mountDir


