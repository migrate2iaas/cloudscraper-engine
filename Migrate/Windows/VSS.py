# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

class VSS(object):
    """Class up to creating and managing VSS snapshots"""
    
    # creates snapshot and returns the snapshot name
    def createSnapshot(self, volumeName):
         raise NotImplementedError

    # deletes the snapshot from system
    def deleteSnapshot(self, snapshotName):
         raise NotImplementedError




