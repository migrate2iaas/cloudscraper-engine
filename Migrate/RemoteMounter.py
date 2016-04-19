"""

This file describes mounter interface

"""


class RemoteMounter(object):
    """Abstract Mounter interface"""


    def __init__(self):
        pass

    def mount(self, directory):
        raise NotImplementedError

    def unmount(self):
        pass

    def isMounted(self):
        pass

    def getMountDir(self):
        pass




class NoRemoteMounter(RemoteMounter):
    """Mounter that does nothing"""


    def __init__(self):
        self.__dir = ""

    def mount(self, directory):
        self.__dir = directory

    def unmount(self):
        self.__dir = ""

    def isMounted(self):
        return self.__dir != ""

    def getMountDir(self):
        return self.__dir


