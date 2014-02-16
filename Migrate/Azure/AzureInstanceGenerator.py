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
   

    def makeInstanceFromImage(self , imageid , initialconfig, instancename):
        """
        Marks the data uploaded as system disk, should be called(?) after the upload is confirmed
        Args:
            imageid :str - hyperlink to page blob vhd media
            initialconfig: AzureCloudOptions - parameters to create instance
            instancename: str - the name of volume instance
        """
        # create volume (registered disk in disks tab)
        volume = self.makeVolumeFromImage(imageid, initialconfig , instancename+"system")
        region = initialconfig.getRegion()
        subnet = initialconfig.getSubnet()
        affinity = None
        network = None
        if subnet:
            network = initialconfig.getZone()
        else:
            affinity = initialconfig.getZone()
        self.__vmService.create_vm(instancename , region, volume , affinity , network, subnet)
        return instancename


    def makeVolumeFromImage(self , imageid , initialconfig, instancename):
        """
        Creates Azure disk
        Args:
            imageid :str - hyperlink to page blob vhd media
            initialconfig: AzureCloudOptions - parameters to create instance
            instancename: str - the name of volume instance
        """
        label = instancename  
        medialink = imageid
        name = instancename + "_disk"

        response = self.__vmService.add_disk(label, medialink, name)
        if response.ok:
            logging.info(">>>>>>>>>>>>>>> Disk " + name + " has been uploaded");
            logging.info("See \'Disks\' tab in Virtual Machines menu of the management portal");
            return name
        else:
            logging.error("!!!ERROR Failed to create VM disk from blob. " + str(response.status_code) + " " + response.reason)
            return None