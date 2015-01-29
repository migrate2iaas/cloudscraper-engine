"""
InstanceGenerator
~~~~~~~~~~~~~~~~~

This module provides InstanceGenerator class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback



class InstanceGenerator(object):
    """Factory class to generate cloud server instances"""

    def __init__(self):
        pass

    def makeInstanceFromImage(self , imageid, initialconfig, instancename):
        """generates cloud server instances from uploaded images"""
        raise NotImplementedError

    def makeVolumeFromImage(self , imageid , initialconfig, instancename):
        """generates cloud server instances from uploaded images"""
        raise NotImplementedError