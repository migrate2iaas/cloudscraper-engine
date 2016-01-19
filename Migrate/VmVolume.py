
# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------



class VmVolume(object):
    """basic interface for a data volume of machine in the cloud"""

    def getId(self):
        """returns cloud id of the volume"""
        raise NotImplementedError

    def attach(self, vm_instance_id):
        """
        Attaches to the machine specified by instance_id
        """
        raise NotImplementedError

    def __str__(self):
        return str(self.getId())