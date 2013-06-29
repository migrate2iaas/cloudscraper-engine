class UploadChannel(object):
    """Interface class of data upload channel"""

    #TODO: make more reliable statistics

    #TODO: we need kinda open method for the channel
    #TODO: need kinda doc
    #chunk size means one data element to be uploaded. it waits till all the chunk is transfered to the channel than makes an upload (not fully implemented)
    def __init__(self):
        return
              
    # uploads an image data extent, could be async. Use waitTillUploadComplete() to wait till all data is uploaded
    def uploadData(self, extent):       
       raise NotImplementedError 

   # gets the size of transfer chunk. All the data except the last chunk should be aligned and be integral of this size
    def getTransferChunkSize(self):
       raise NotImplementedError

    #returns float: number of bytes transfered per second
    def getDataTransferRate(self):
       raise NotImplementedError

    # gets overall data skipped 'cause it was uploaded earlier
    def getOverallDataSkipped(self):
        raise NotImplementedError

    # gets path describing the channel. Could be used to resume uploading if diconnected
    def getUploadPath(self):
        raise NotImplementedError

    #TODO: this ones should have generic implemenation 

    #gets the overall size of data uploaded
    def getOverallDataTransfered(self):
        raise NotImplementedError

    # waits till upload is complete
    def waitTillUploadComplete(self):
        raise NotImplementedError

    # confirms good upload. returns id of the image created
    def confirm(self):
        raise NotImplementedError

    # closes the channel
    def close(self):
        raise NotImplementedError
   

