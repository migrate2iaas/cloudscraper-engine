"""File declaring interface to upload disk images to cloud via several subsequent channels"""
# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import DataExtent
import logging
import UploadChannel


class ChainUploadChannel(UploadChannel.UploadChannel):
    """
    interface to upload disk images to cloud via several subsequent channels
    The first one is to upload local image to an online storage while next ones are to call outer services to move data into the cloud block storage
    """

    def __init__(self):
        """constructor"""
        self.__channelsList = list()
        super(ChainUploadChannel, self).__init__()

    def appendChannel(self, channel):
        self.__channelsList.append(channel)
              
    def uploadData(self, extent):       
       """ 
       Uploads image data extent, 
       Could be async. Use waitTillUploadComplete() to wait till all data is uploaded

       Args:
            extent: DataExtent - extent of the image to be uploaded. Its size and offset should be integral of chunk size. The only exception is last chunk. 
       """
       if len(self.__channelsList) > 0:
           result = self.__channelsList[0].uploadData(extent)
       else:
           raise KeyError("Cannot upload data. No channels added to the chain!")
        # maybe to add upload (fake?) chunks to subsequent channels here too?
        # or start confirming and subsequent uploading here on the last chunk?
       
       if result == False or extent.getStart() + extent.getSize() < self.getImageSize():
           return result

       # in case we've completed transfering stuff
       channel_index = 0
       while channel_index < len(self.__channelsList):
           logging.info("Starting the next upload channel in the chain")
           if channel_index + 1 < len(self.__channelsList): 
               # complete and confirm all channels besides the last one
               self.__channelsList[channel_index].waitTillUploadComplete()
               intermediate_id = self.__channelsList[channel_index].confirm()
               if not intermediate_id:
                   logging.error("!!!ERROR: failed to the finalize the intermediate upload")
                   return False
               #  maybe to upload (fake?) chunks here too?
               self.__channelsList[channel_index + 1].initStorage(intermediate_id)
           channel_index = channel_index + 1

       return True

    def getTransferChunkSize(self):
       """
       Gets the size of transfer chunk in bytes.
       All the data except the last chunk should be aligned and be integral of this size    
       """
       if len(self.__channelsList) > 0:
           return self.__channelsList[0].getTransferChunkSize()
          
       raise KeyError("Cannot get chunk size. No channels added to the chain!")

    def waitTillUploadComplete(self):
        """
        Client calls this method to wait till all async upload is complete.
        Chain upload channel inits next channels in the chain in this funciton
        """
        if len(self.__channelsList) > 0:
           # wait the last channel to complete
           self.__channelsList[-1].waitTillUploadComplete()
        else:
            raise KeyError("Cannot wait upload complete. No channels added to the chain!")
        

    def confirm(self):
        """
        Is called after waitTillUploadComplete() to ensure the upload task is complete. 

        Return:
             Cloud disk image identifier: str - in case of success or
             None - in case of failure
        """
        # the last result is the id suitable to create an instance in the cloud
        if len(self.__channelsList) > 0:
           # wait the last channel to complete
           return self.__channelsList[-1].confirm()
        else:
            raise KeyError("Cannot confirm. No channels added to the chain!")


    def getUploadPath(self):
        """
        Gets string representing the channel, e.g. Amazon bucket and keyname
        """
        if len(self.__channelsList) > 0:
           return self.__channelsList[0].getUploadPath()
          # maybe to add upload (fake?) chunks to subsequent channels here too?
        raise KeyError("Cannot access channel. No channels added to the chain!")

    def getDataTransferRate(self):
       """ 
       Return: 
            float: approx. number of bytes transfered per second 
       """
       if len(self.__channelsList) > 0:
          return self.__channelsList[0].getDataTransferRate()
         # maybe to add upload (fake?) chunks to subsequent channels here too?
       raise KeyError("Cannot access channel. No channels added to the chain!")

    def getOverallDataSkipped(self):
        """
        Gets overall size of data skipped in bytes. 
        Data is skipped by the channel when the block with same checksum is already present in the cloud
        """
        if len(self.__channelsList) > 0:
          return self.__channelsList[0].getOverallDataSkipped()
         # maybe to add upload (fake?) chunks to subsequent channels here too?
        raise KeyError("Cannot access channel. No channels added to the chain!")

    def getOverallDataTransfered(self):
        """
        Gets overall size of data actually uploaded (not skipped) in bytes.
        """
        if len(self.__channelsList) > 0:
          return self.__channelsList[0].getOverallDataTransfered()
         # maybe to add upload (fake?) chunks to subsequent channels here too?
        raise KeyError("Cannot access channel. No channels added to the chain!")

    def close(self):
        """
        Closes the channel, deallocates any associated resources
        """
        for channel in self.__channelsList:
            channel.close()
   

    def initStorage(self, init_data_link=""):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """
        if len(self.__channelsList) > 0:
          return self.__channelsList[0].initStorage(init_data_link)
         # maybe to add upload (fake?) chunks to subsequent channels here too?
        raise KeyError("Cannot access channel. No channels added to the chain!")

    def getImageSize(self):
        """
        Gets image data size to be uploaded
        """
        if len(self.__channelsList) > 0:
          return self.__channelsList[0].getImageSize()
         # maybe to add upload (fake?) chunks to subsequent channels here too?
        raise KeyError("Cannot access channel. No channels added to the chain!")

    def getDiskUploadedProperty(self):
        """
        Returns amount of data already uploaded as it saved in the cloud storage
        This data could be loaded from the disk object on cloud side which channel represents
        """
        if len(self.__channelsList) > 0:
          return self.__channelsList[0].getDiskUploadedProperty()
         # maybe to add upload (fake?) chunks to subsequent channels here too?
        raise KeyError("Cannot access channel. No channels added to the chain!")

    def __loadDiskUploadedProperty(self):
        """
        Loads data already uploaded property as it saved in the cloud storage
        Returns False if disk property could be loaded, True if it was loaded and saved, excepts otherwise
        """
        if len(self.__channelsList) > 0:
          return self.__channelsList[0].__loadDiskUploadedProperty()
         # maybe to add upload (fake?) chunks to subsequent channels here too?
        raise KeyError("Cannot access channel. No channels added to the chain!")