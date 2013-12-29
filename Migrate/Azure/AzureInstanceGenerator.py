"""
AzureInstanceGenerator
~~~~~~~~~~~~~~~~~

This module provides AzureInstanceGenerator class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import virtualmachine
import InstanceGenerator


class AzureInstanceGenerator(InstanceGenerator.InstanceGenerator):
    """Generates Azure Instances (just links to VM disks for now)"""

    def __init__(self , subscription, certpath):

        self.__vmService = virtualmachine.virtualmachine(certpath , subscription )
        

     # marks the data uploaded as system disk, should be called(?) after the upload is confirmed
    #TODO: should pass name here
    def makeInstanceFromImage(self , imageid , initialconfig, instancename):
       
        label = instancename + "-system.vhd"
        medialink = imageid
        name = instancename + "-system.vhd"

        response = self.__vmService.add_disk(label, medialink, name)
        if response.ok:
            logging.info(">>>>>>>>>>>>>>> Disk " + name + " has been uploaded, see Disks tab in Virtual Machines menu of the management portal");
            return name
        else:
            logging.error("!!!ERROR Failed to create VM disk from blob" + str(response.status_code) + " " + response.reason)
            return None


