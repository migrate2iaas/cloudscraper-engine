"""File declaring interface to upload disk images to cloud storage"""
# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import DataExtent

class UploadChannel(object):
    """Interface class of disk image data upload channel"""

    def __init__(self):
        """constructor"""
        return
              
    def uploadData(self, extent):       
       """ 
       Uploads image data extent, 
       Could be async. Use waitTillUploadComplete() to wait till all data is uploaded

       Args:
            extent: DataExtent - extent of the image to be uploaded. Its size and offset should be integral of chunk size. The only exception is last chunk. 
       """
       raise NotImplementedError 

    def getTransferChunkSize(self):
       """
       Gets the size of transfer chunk in bytes.
       All the data except the last chunk should be aligned and be integral of this size    
       """
       raise NotImplementedError

    def waitTillUploadComplete(self):
        """
        Client calls this method to wait till all async upload is complete
        """
        raise NotImplementedError

    def confirm(self):
        """
        Is called after waitTillUploadComplete() to ensure the upload task is complete. 

        Return:
             Cloud disk image identifier: str - in case of success or
             None - in case of failure
        """
        raise NotImplementedError


    def getUploadPath(self):
        """
        Gets string representing the channel, e.g. Amazon bucket and keyname
        """
        raise NotImplementedError

    def getDataTransferRate(self):
       """ 
       Return: 
            float: approx. number of bytes transfered per second 
       """
       raise NotImplementedError

    def getOverallDataSkipped(self):
        """
        Gets overall size of data skipped in bytes. 
        Data is skipped by the channel when the block with same checksum is already present in the cloud
        """
        raise NotImplementedError

    def getOverallDataTransfered(self):
        """
        Gets overall size of data actually uploaded (not skipped) in bytes.
        """
        raise NotImplementedError

    def close(self):
        """
        Closes the channel, deallocates any associated resources
        """
        raise NotImplementedError
   

    def initStorage(self):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """
        return

    def getImageSize(self):
        """
        Gets image data size to be uploaded
        """
        raise NotImplementedError