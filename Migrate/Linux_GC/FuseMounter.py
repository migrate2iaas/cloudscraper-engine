"""

This file describes FUSE mount

The intention is create store data in a FUSE-based filesystem

"""

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import re
from subprocess import *
import RemoteMounter
import os
import threading

class FuseMounter(RemoteMounter.RemoteMounter):
    """Fuse mount is a class to start Fuse-based filesystems"""


    def __init__(self , fuse_operations):
        self.__mountDir = ""
        self.__fuseCallbackObj = fuse_operations
        self.__fuse = None

    def __mount_thread(self):
        # TODO: cancel\notify on mount end
        self.__fuse = FUSE(self.__fuseCallbackObj, self.__mountDir, foreground=True)

    def mount(self, directory):
        self.__mountDir = directory 
        # TODO:	modprobe fuse
        os.mkdir(directory , 0700)
        threading.Thread(target = self.__mount_thread , args = ())
        

    def unmount(self):
        p1 = Popen(["umount" , self.__mountDir], stdout=PIPE)
        output = p1.communicate()[0]
        self.__mountDir = ""
        #TODO: umount dir

    def isMounted(self):
        return self.__mountDir != ""

    def getMountDir(self):
        return self.__mountDir


