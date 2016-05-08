"""
MultithreadedSwiftUploadChannel
~~~~~~~~~~~~~~~~~

This module provides MultithreadedSwiftUploadChannel class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2016 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import MultithreadUpoadChannel



class SwiftUploadTask(MultithreadUpoadChannel.UploadTask):
    def __init__(self , channel , extent):
        """simple constructor"""
        self.__extent = extent
        size = extent.getSize()
        offset = extent.getStart()
        return super(SwiftUploadTask, self).__init__(channel , size , offset)

    def getData(self):
        return self.__extent.getData()

    def getExtent(self):
        return self.__extent


class MultithreadedSwiftUploadChannel(MultithreadUpoadChannel.MultithreadUpoadChannel):
    """Composite class (SwiftUploadChannel_new + MultithreadedUploadChannel). 
        Most of time it proxies existing swift channel but doing keeps multithreaded queue impl of the upload process
    """

   

    def __init__(self, swift_channel , result_disk_size_bytes, upload_threads=12 , queue_size=16 , sync_every_requests = 16):
        """takes exisitng swift channel and wraps up it with mulitithreaded upload queue logic
        Args:
            swift_channel - existing channel. The caller must take care the channel is single threaded
            result_disk_size_bytes - disk size in bytes
        """
        self.__swift = swift_channel
        return super(MultithreadedSwiftUploadChannel, self).__init__(result_disk_size_bytes , upload_threads = upload_threads, queue_size = queue_size , sync_every_requests=sync_every_requests)

    def initStorage(self, init_data_link=""):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """
        return self.__swift.initStorage(init_data_link=init_data_link)

    def getUploadPath(self):
        """
        Gets string representing the channel, e.g. Amazon bucket and keyname that could be used for upload.
        Needed mainly in diagnostics purposes
        """
        return self.__swift.getUploadPath()

    def createUploadTask(self , extent):
        """ 
        Protected method. Creates upload task to add it the queue 
        Returns UploadTask (or any of inherited class) object

        Args:
            extent: DataExtent - extent of the image to be uploaded. Its size and offset should be integral of chunk size. The only exception is last chunk. 

        """
        return SwiftUploadTask(self , extent)

    
    def confirm(self):
        """
        Registers the image in cloud
        Note, call confirm() only after waitTillUploadComplete() to ensure the upload task is complete.

        Return:
             Cloud uploaded disk image identifier that could be passed to Cloud API to create new server: str - in case of success or
             None - in case of failure
        """
        return self.__swift.confirm()

    def uploadChunk(self , uploadtask):
        """
        Protected. Uploads one chunk of data
        Called by the worker thread internally.
        Could throw any non-recoverable errors
        """
        return self.__swift.uploadData(uploadtask.getExtent())