

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

    def getSystemDiskType(self):
        raise NotImplementedError

   

     