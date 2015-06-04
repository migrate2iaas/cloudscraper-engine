"""
FsBundler
~~~~~~~~~~~~~~~~~

This module provides FsBundler class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import sys
import os
sys.path.append('./../compute-image-packages/gcimagebundle')
sys.path.append('./../../compute-image-packages/gcimagebundle')
from gcimagebundlelib import *
from gcimagebundlelib import utils
from gcimagebundlelib import platform_factory
from gcimagebundlelib import block_disk
from gcimagebundlelib import exclude_spec
from gcimagebundlelib import os_platform
from gcimagebundlelib import imagebundle


import NbdBundle_utils

class FsBundler(block_disk.RootFsRaw):
    """extended implementation of Bundleup logics"""

    def __init__(self, fs_size, fs_type, skip_disk_space_check, diskname = "disk.raw" , statvfs = os.statvfs):
        #override to use ndb
        NbdBundle_utils.NbdOverride.init_override()

        super(FsBundler , self).__init__(fs_size, fs_type, skip_disk_space_check, statvfs , diskname)

    def Bundleup(self):
        return super(FsBundler, self).Bundleup()
        

    def _InitializeDiskFileFromDevice(self , filepath):
        with NbdBundle_utils.LoadNbdImage(filepath) as path:
            return super(FsBundler, self)._InitializeDiskFileFromDevice(path)
        

    def _ResizeFile(self, file_path, file_size):
        # NOTE: we assume that disk size is okay to keep all data
        return
        #gcimagebundlelib.utils.RunCommand(["qemu-img" ])
        #return super(FsBundler, self)._ResizeFile(file_path, file_size)


