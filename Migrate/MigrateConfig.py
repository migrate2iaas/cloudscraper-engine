# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

class ConfigAccessor(object):
    """base class to save the config value"""

    def saveValue(self , valuename , value):
        return 
    def getValue(self , valuename , value):
        return


#TODO: implement the config saver
# add save to config here
# so the configurer adds 
class VolumeMigrateConfig(object):
    """ base class for volume migration parms """


    def getImagePath(self):
        raise NotImplementedError

    def getUploadPath(self):
        raise NotImplementedError

    def getUploadId(self):
        raise NotImplementedError

    def getImageSize(self):
        raise NotImplementedError

    def getVolumePath(self):
        raise NotImplementedError

    # returns iterable of excluded dirs
    # NOTE: some more sophisticated exclude system is to be designed
    # if this mechanism is to be really used
    def getExcludedDirs(self):
        raise NotImplementedError

    def setUploadPath(self, path):
        raise NotImplementedError

    def setUploadId(self , uploadid):
        raise NotImplementedError

    def setImagePath(self , imagepath):
        raise NotImplementedError

    def setImageSize(self , size):
        raise NotImplementedError
   
    def saveConfig(self):
        raise NotImplementedError


class MigrateConfig(object):
    """ base class for the migration config"""

    def __init__(self):
        return

    #local system or the pre-created image. No fixups on images are currently supported
    def getSourceOs(self):
        raise NotImplementedError

    def getHostOs(self):
        raise NotImplementedError

    def getImageType(self):
        raise NotImplementedError
    
    def getImagePlacement(self):
        raise NotImplementedError

    def getSystemImagePath(self):
        raise NotImplementedError

    def getSystemImageSize(self):
        raise NotImplementedError

    def getSystemConfig(self):
        raise NotImplementedError

    def getSystemVolumeConfig(self):
        raise NotImplementedError

    def getSystemDiskType(self):
        raise NotImplementedError

    # gets list of VolumeMigrateConfig
    def getDataVolumes(self):
        raise NotImplementedError
   

     