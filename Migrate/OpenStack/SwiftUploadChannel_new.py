﻿# --------------------------------------------------------
__author__ = "Alexey Kondratiev"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('.\..')
sys.path.append('.\..\OpenStack')
sys.path.append('.\OpenStack')

import os 
import hashlib
import Queue
import time
import io

import MigrateExceptions
import traceback

import StringIO
import logging
import threading 
import UploadChannel
import DataExtent

import json
from md5 import md5

import swiftclient.client
from swiftclient.exceptions import ClientException

class DefferedUploadFileProxy(object):
    def __init__(self, queue_size, size):
        self.__inner_queue = Queue.Queue(queue_size)
        self.__cancel = False
        self.__size = size
        self.__readed_size = 0
        self.__md5encoder = md5()
        self.__completed = threading.Event()

    def read(self, len):
        if self.cancelled() or self.__readed_size == self.__size:
            return ""

        extent = self.__inner_queue.get()
        logging.debug("Reading data extent: start: " + str(extent.getStart()) + " size: " + str(extent.getSize()))

        data = extent.getData()
        self.__md5encoder.update(data)
        self.__readed_size += data.__len__()
        self.__inner_queue.task_done()

        return data

    def write(self, extent):
        if not self.__cancel:
            self.__inner_queue.put(extent)

    def getSize(self):
        return self.__size

    def getCompletedSize(self):
        return self.__readed_size

    def setComplete(self):
        self.__completed.set()

    def waitTillComplete(self):
        if not self.cancelled():
            self.__completed.wait()

    def getMD5(self):
        return self.__md5encoder.hexdigest()

    def cancel(self):
        if not self.__cancel:
            with self.__inner_queue.mutex:
                self.__cancel = True
                self.__inner_queue.queue.clear()
                self.setComplete()

    def cancelled(self):
        return self.__cancel

class SwiftUploadThread(threading.Thread):
    """thread making all uploading works"""
    def __init__(self, upload_channel, file_proxy, index, file_lock):
        self.__uploadChannel = upload_channel
        self.__fileProxy = file_proxy
        self.__index = index
        self.__fileLock = file_lock
        super(SwiftUploadThread, self).__init__()

    def run(self):
        if self.__uploadChannel.skipExisting():
            logging.debug("Upload thread started with reuploading turned on")

        # Trying to connect to swift
        connection = None
        try:
            connection = self.__uploadChannel.createConnection()
        except Exception:
            raise

        res = {
            'action': 'empty',
            'success': False,
            'index': self.__index,
            'path': "%s/slo/%08d" % (self.__uploadChannel.getDiskName(), self.__index),
            'etag': None,
            'size': self.__fileProxy.getSize(),
        }

        try:
            is_exists = False
            # Trying to check etag for existing segment
            try:
                if self.__uploadChannel.skipExisting():
                    # Trying to find segment in resume upload list, which was loaded from disk
                    resume_segments = self.__uploadChannel.getResumeSegmentResults()
                    for i in resume_segments:
                        if res['index'] == i['index']:
                            res.update({'etag': i['etag']})
                            break

                    headers = connection.head_object(self.__uploadChannel.getContainerName(), res['path'])
                    if headers['etag'] == res['etag']:
                        res.update({
                            'action': 'skip_segment',
                            'success': True
                        })
                        is_exists = True
            except ClientException:
                # Passing exception here, it's means that when we unable to check
                # uploaded segment (it's missing or etag mismatch) we reuploading that segment
                pass

            results_dict = {}
            if is_exists is False:
                etag = connection.put_object(
                    self.__uploadChannel.getContainerName(),
                    res['path'],
                    self.__fileProxy,
                    chunk_size=self.__uploadChannel.getChunkSize(),
                    response_dict=results_dict)
                segment_md5 = self.__fileProxy.getMD5()
                if etag != segment_md5:
                    raise ClientException(
                        'Segment {0}: upload verification failed: '
                        'md5 mismatch, local {1} != remote {2} '
                        '(remote segment has not been removed)'
                        .format(segment['path'], segment_md5, etag))

                res.update({
                    'success': True,
                    'action': 'upload_segment',
                    'etag': etag
                })

            self.__uploadChannel.appendSegmentResult(res)
            # Dumping segment results for resuming upload, if needed
            with self.__fileLock:
                with open(self.__uploadChannel.getContainerName() + '.' + self.__uploadChannel.getDiskName() + '.txt', 'w') as file:
                    json.dump(self.__uploadChannel.getSegmentResults(), file)

            # Notify that upload complete
            self.__fileProxy.setComplete()

        except (ClientException, Exception) as err:
            self.__fileProxy.cancel()
            logging.error(traceback.format_exc())
            logging.debug("Exception in upload thread for '%08d'" % self.__index)

        self.__uploadChannel.completeUploadThread()
        connection.close()

        logging.debug("Upload thread for '%08d' done" % self.__index)


class SwiftUploadChannel_new(UploadChannel.UploadChannel):
    """
    Upload channel for Swift implementation
    Implements multithreaded fie upload to Openstack Swift
    """

    def __init__(
            self,
            resulting_size_bytes,
            server_url,
            username,
            tennant_name,
            password,
            disk_name,
            container_name,
            retries=3,
            compression=False,
            resume_upload=False,
            chunksize=10*1024*1024,
            upload_threads=10,
            queue_size=8):
        """constructor"""
        self.__serverURL = server_url;
        self.__userName = username;
        self.__tennantName = tennant_name;
        self.__password = password;
        self.__retries = retries;
        self.__compression = compression;
        self.__containerName = container_name
        self.__diskName = disk_name
        self.__chunkSize = chunksize
        self.__diskSize = resulting_size_bytes
        self.__resumeUpload = resume_upload
        self.__uploadThreads = threading.BoundedSemaphore(upload_threads)
        self.__segmentQueueSize = queue_size
        self.__segmentsList = []
        self.__resumeSegmentsList = []
        self.__fileProxies = []
        self.__fileLock = threading.Lock()

        # Max segment number is 1000 (it's configurable see http://docs.openstack.org/developer/swift/middleware.html )
        self.__segmentSize = max(int(self.__diskSize / 512), self.__chunkSize)
        if self.__segmentSize % self.__chunkSize:
            # Make segment size an integer of chunks
            self.__segmentSize = self.__segmentSize - (self.__segmentSize % self.__chunkSize)

        logging.info("Segment size: " + str(self.__segmentSize) + " chunk size: " + str(self.__chunkSize))

        # Loading segment results if resuming upload, clearing file otherwise
        open_opt = 'w'
        if self.__resumeUpload:
            open_opt = 'r'
        try:
            with open(self.__containerName + '.' + self.__diskName + '.txt', open_opt) as file:
                self.__resumeSegmentsList = json.load(file)
        except Exception:
            pass

        super(SwiftUploadChannel_new, self).__init__()

    def uploadData(self, extent):
        logging.debug("Uploading extent: start: " + str(extent.getStart()) + " size: " + str(extent.getSize()))

        index = extent.getStart() / self.__segmentSize
        offset = extent.getStart() % self.__segmentSize

        # If needed to upload new segment
        if offset == 0:
            # Checking if we can create more upload threads, they releases when calls completeUploadThread() routine
            self.__uploadThreads.acquire()
            logging.debug("Starting new upload thread for '%08d'" % index)

            # Checking if this is the last segment
            segment_size = 0;
            if extent.getStart() + self.__segmentSize > self.__diskSize:
                segment_size = self.__diskSize - extent.getStart()
            else:
                segment_size = self.__segmentSize
            self.__fileProxies.insert(index, DefferedUploadFileProxy(self.__segmentQueueSize, segment_size))
            SwiftUploadThread(self, self.__fileProxies[index], index, self.__fileLock).start()

        self.__fileProxies[index].write(extent)

        return True

    def completeUploadThread(self):
        self.__uploadThreads.release()

    def getSegmentResults(self):
        return self.__segmentsList

    def getResumeSegmentResults(self):
        return self.__resumeSegmentsList

    def appendSegmentResult(self, result):
         return self.__segmentsList.append(result)

    def getContainerName(self):
        return self.__containerName

    def getDiskName(self):
        return self.__diskName

    def skipExisting(self):
        return self.__resumeUpload

    def getChunkSize(self):
        return self.__chunkSize

    def createConnection(self):
        return swiftclient.client.Connection(
            self.__serverURL,
            self.__tennantName + ':' + self.__userName,
            self.__password,
            self.__retries,
            auth_version='2',
            snet=False,
            ssl_compression=self.__compression,
            timeout=86400)

    def initStorage(self, init_data_link=""):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """
        res = {
            'success': False,
            'headers': None,
            'container': self.__containerName,
        }

        resp_dict = {}
        try:
            connection = self.createConnection()
            res['action'] = 'put_container'
            connection.put_container(res['container'], headers=res['headers'], response_dict=resp_dict)

            res['action'] = 'post_container'
            res['headers'] = {'X-Container-Read': '.r:*'}
            connection.post_container(res['container'], headers=res['headers'], response_dict=resp_dict)

            res['success'] = True
        except (ClientException, Exception) as err:
            logging.error("!!!ERROR: " + err.message)
        finally:
            connection.close()

        return res['success']


    def getUploadPath(self):
        """
        Gets the upload path identifying the upload sufficient to upload the disk in case storage account and
        container name are already defined
        """
        return self.__diskName


    def getTransferChunkSize(self):
        return self.__chunkSize


    def waitTillUploadComplete(self):
        """Waits till upload completes"""
        logging.debug("Upload complete, waiting for threads to complete");
        # Waiting till upload queues in all proxy files becomes empty
        for item in self.__fileProxies:
            item.waitTillComplete()
        logging.info("Upload threads are completed, closing threads")
        self.close()
        return


    def confirm(self):
        """
        Confirms good upload
        """
        logging.info("Confirming good upload")
        storage_url = None
        try:
            # If segments size and disk size mismatch
            total_size = 0;

            # If segment dictionary has error:
            for i in self.__segmentsList:
                if i['success'] == False:
                    raise ClientException("Failure due uploading segment(s)")
                else:
                    total_size += i['size']

            if total_size != self.__diskSize:
                raise ClientException("Failure due uploading segments: disk size mismatch")

            # Segments can upload not in sequential order, so we need to sort them for manifest
            self.__segmentsList.sort(key=lambda di: di['index'])

            # Creating manifest
            manifest_data = json.dumps([{
                    'path': self.__containerName + '/' + d['path'],
                    'etag': d['etag'],
                    'size_bytes': d['size']
            } for d in self.__segmentsList])

            mr = {}
            connection = self.createConnection()
            connection.put_object(
                self.__containerName,
                self.__diskName,
                manifest_data,
                headers={'x-static-large-object': 'true'},
                query_string='multipart-manifest=put',
                response_dict=mr
            )
            storage_url = connection.url + '/' + self.__containerName + '/' + self.__diskName
            connection.close()
        except (ClientException, Exception) as err:
            logging.error("!!!ERROR: " + err.message)
            raise

        return storage_url


    def getTransferChunkSize(self):
       """
       Gets the size of transfer chunk in bytes.
       All the data except the last chunk should be aligned and be integral of this size
       """
       return self.__chunkSize


    def getDataTransferRate(self):
       """
       Return:
            float: approx. number of bytes transfered per second
       """
       return 0


    def getOverallDataSkipped(self):
        """
        Gets overall size of data skipped in bytes.
        Data is skipped by the channel when the block with same checksum is already present in the cloud
        """
        return 0


    def getOverallDataTransfered(self):
        """
        Gets overall size of data actually uploaded (not skipped) in bytes.
        """
        completed_size = 0;
        try:
            for file in self.__fileProxies:
                completed_size += file.getCompletedSize()
        except Exception as err:
            logging.debug("Unable to calculete completed data size: " + err.message())

        return completed_size


    def getImageSize(self):
        """
        Gets image data size to be uploaded
        """
        return self.__diskSize


    def getDiskUploadedProperty(self):
        """
        Returns amount of data already uploaded as it saved in the cloud storage
        This data could be loaded from the disk object on cloud side which channel represents
        """
        return 0


    def close(self):
        """Closes the channel, sending all upload threads signal to end their operation"""
        logging.debug("Closing the upload threads, End signal message was sent")