# --------------------------------------------------------
__author__ = "Alexey Kondratiev"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('.\..')
sys.path.append('.\..\OpenStack')
sys.path.append('.\OpenStack')

import os 
import hashlib
import threading
import Queue
import DataExtent
import time
import io
import tempfile
import warnings

import sys
import os
import MigrateExceptions
import subprocess
import re
import traceback

import zlib
import gzip
import StringIO

import logging
import threading 
import datetime
import UploadChannel
import MultithreadUpoadChannel
import DataExtent

import base64
import math
import json
import pickle
from md5 import md5

import swiftclient.client
from swiftclient.service import get_conn, process_options #, split_headers
from swiftclient.command_helpers import stat_account
from swiftclient.service import _default_global_options, _default_local_options
from swiftclient.service import SwiftError#, SwiftUploadObject
from swiftclient.exceptions import ClientException
#from swiftclient.client import Connection
from swiftclient.utils import generate_temp_url

class DefferedMultifileUploadFileProxy(object):
    def __init__(self , deffered_data):
        self.__pos = 0
        self.__data = deffered_data
        self.__md5encoder = md5()

    def read(self , len):
        data = self.__data.readData(len , self.__pos)
        self.__pos += len
        self.__md5encoder.update(data)
        return data

    def write(self, buf):
        raise NotImplementedError

    def close(self):
        pass

    def size(self):
        return self.__data.getSize()

    def chunk_size(self):
        return self.__data.getChunkSize()

    def seek(self, pos, whence=0):
        self.__pos = pos

    def cancel(self):
        self.__data.cancel()

    def getMD5(self):
        return self.__md5encoder.hexdigest()


class DefferedUploadDataStream(object):

    opened_streams = dict()
    @staticmethod
    def getFileProxy(name):
        return DefferedMultifileUploadFileProxy(DefferedUploadDataStream.opened_streams[name])

    @staticmethod
    def getStream(name):
        return DefferedUploadDataStream.opened_streams[name]

    def __init__(self , name , size, chunksize, semaphore):
        self.__name = name
        self.__pos = 0
        self.__size = size
        self.__parts = dict()
        self.__chunksize = chunksize
        DefferedUploadDataStream.opened_streams[name] = self
        self.__semaphore = semaphore
        self.__dictLock = threading.Lock()
        self.__cancel = False
        self.__completeDataSize = 0
        self.__writeCount = 0

    def readData(self, len , pos):
        if self.__completeDataSize == self.__size:
            return ""

        while True:
            interval_start = self.__chunksize * int(pos / self.__chunksize)
            with self.__dictLock:
                if self.__parts.has_key(interval_start):
                    #logging.debug("Getting data at pos " + str(pos) + " of len " + str(len) )
                    part = self.__parts[interval_start]
                    data = part.getData()[pos-interval_start:pos-interval_start+len] # now we read all data from the extent
                    if pos-interval_start+len >= self.__chunksize:
                        del self.__parts[interval_start]
                        self.__semaphore.release()
                        logging.debug("Removed entry from deffered upload stream '" + self.__name + "' at pos " + str(interval_start))
                    if data == None:
                        logging.warning("!No data available")
                        data = ""
                    self.__completeDataSize = self.__completeDataSize + data.__len__()
                    return data
            # not sure if it works. Usually sleep is the worst method to do multithreading
            # we just hope network calls for data not so often
            time.sleep(1)
            logging.debug("Waiting till data is available for stream '" + self.__name + "' at pos " + str(pos))

    def writeData(self, extent):
        pos = extent.getStart()
        logging.debug("Adding more data to deffered upload stream '" + self.__name + "' at pos " + str(pos))
        self.__semaphore.acquire()
        if self.cancelled():
            return
        logging.debug("Added more data to deffered upload stream '" + self.__name + "' at pos " + str(pos))
        with self.__dictLock:
            self.__parts[pos] = extent

    def seek(self, pos):
        self.__pos = pos

    def getName(self):
        return self.__name

    def getSize(self):
        return self.__size

    def getChunkSize(self):
        return self.__chunksize

    def getCompletedSize(self):
        return self.__completeDataSize

    def cancel(self):
        self.__cancel = True
        # not the best practice but the only way to unblock the writing trhead
        self.__semaphore.release()

    def cancelled(self):
        return self.__cancel

class SwiftUploadQueueTask(object):
    def __init__(self, container_name, disk_name, stream_proxy, channel):
        self.__containerName = container_name
        self.__diskName = disk_name
        self.__streamProxy = stream_proxy
        self.__channel = channel

    def getSwiftConnection(self):
        return self.__swiftConnection

    def getContainerName(self):
        return self.__containerName

    def getDiskName(self):
        return self.__diskName

    def getStreamProxy(self):
        return self.__streamProxy

    def getChannel(self):
        return self.__channel


class SwiftUploadThread(threading.Thread):
    """thread making all uploading works"""
    def __init__(self, connection, queue, lock, skip_existing = False):
        self.__swiftConnection = connection
        self.__uploadQueue = queue
        self.__skipExisting = skip_existing
        self.__fileLock = lock
        super(SwiftUploadThread, self).__init__()

    def run(self):
        if self.__skipExisting:
            logging.debug("Upload thread started with reuploading turned on")

        while True:
            upload_task = self.__uploadQueue.get()

            # Means it's time to exit
            if upload_task == None:
                return

            results_dict = {}
            stream_proxy = upload_task.getStreamProxy()
            index = int(stream_proxy.getName())
            file_proxy = stream_proxy.getFileProxy(stream_proxy.getName())

            try:
                seg_res = upload_task.getChannel().getSegmentResults()
                segment = next(i for i in seg_res if i['index'] == index)
                is_exists = False

                # Trying to hey etag for existing segment
                try:
                    if self.__skipExisting:
                        headers = self.__swiftConnection.head_object(upload_task.getContainerName(), segment['path'])
                        if headers['etag'] == segment['etag']:
                            segment.update({
                                'action': 'skip_segment',
                                'success': True
                            })
                            is_exists = True
                except ClientException:
                    pass

                results_dict = {}
                if is_exists is False:
                    etag = self.__swiftConnection.put_object(
                        upload_task.getContainerName(),
                        segment['path'],
                        file_proxy,
                        chunk_size=stream_proxy.getChunkSize(),
                        response_dict=results_dict)
                    segment_md5 = file_proxy.getMD5()
                    if etag != segment_md5:
                        raise ClientException(
                            'Segment {0}: upload verification failed: '
                            'md5 mismatch, local {1} != remote {2} '
                            '(remote segment has not been removed)'
                            .format(segment['path'], segment_md5, etag))

                    segment.update({
                        'success': True,
                        'action': 'upload_segment',
                        'etag': etag
                    })

                # Dumping segment results for resuming upload, if needed
                try:
                    self.__fileLock.acquire()
                    with open(upload_task.getContainerName() + '.' + upload_task.getDiskName() + '.txt', 'w') as file:
                        json.dump(seg_res, file)
                finally:
                    self.__fileLock.release()

            except (ClientException, Exception) as err:
                logging.error("!!!ERROR: " + err.message)
                _traceback = traceback.format_exc()
                logging.error(_traceback)
                segment.update({'exception': {'message': err, 'traceback': _traceback}})
            finally:
                break
                file_proxy.cancel()

            self.__uploadQueue.task_done()

class SwiftUploadChannel_new(UploadChannel.UploadChannel):
    """
    Upload channel for Swift implementation
    Implements multithreaded fie upload to Openstack Swift
    """

    def __init__(
            self,
            resulting_size_bytes,
            server_url,
            user_name,
            tennant_name,
            password,
            disk_name,
            container_name,
            retries=3,
            compression=False,
            resume_upload=False,
            chunksize=1024*1024,
            upload_threads=4,
            queue_size=8):
        """constructor"""
        self.__containerName = container_name
        self.__diskName = disk_name
        self.__chunkSize = chunksize
        self.__diskSize = resulting_size_bytes
        self.__resumeUpload = resume_upload
        self.__segmentFutures = []

        # Max segment number is 1000 (it's configurable see http://docs.openstack.org/developer/swift/middleware.html )
        self.__segmentSize = max(int(self.__diskSize / 512), self.__chunkSize)
        if self.__segmentSize % self.__chunkSize:
            # Make segment size an integer of chunks
            self.__segmentSize = self.__segmentSize - (self.__segmentSize % self.__chunkSize)

        # self.__uploadedSize = 0

        # Minimum file size to upload using static large object is 1mb
        # (see http://docs.openstack.org/developer/swift/middleware.html)

        # Loading segment results if resuming upload, clearing file otherwise
        open_opt = 'w'
        if self.__resumeUpload:
            open_opt = 'r'
        try:
            with open(self.__containerName + '.' + self.__diskName + '.txt', open_opt) as file:
                self.__segmentFutures = json.load(file)
        except Exception:
            pass

        # Creating all proxy segments and put them to queue
        self.__uploadQueue = Queue.Queue()

        offset = 0
        segment_size = self.__segmentSize
        chunksize = self.__chunkSize
        semaphore = threading.Semaphore(upload_threads * queue_size)
        while offset < self.__diskSize:
            if self.__diskSize - offset < segment_size:
                segment_size = self.__diskSize - offset

            index = '%08d' % int(offset / self.__segmentSize)
            # If resuming upload, we doesnt need to append new segments,
            # to avoid creating them twice in the list
            if self.__resumeUpload is False:
                res = {
                    'action': 'empty',
                    'success': False,
                    'exception': {'message': None, 'traceback': None},
                    'index': int(index),
                    'path': '%s/slo/%s' % (self.__diskName, index),
                    'etag': None,
                    'size': segment_size}
                self.__segmentFutures.append(res)
            stream = DefferedUploadDataStream(index, segment_size, chunksize, semaphore)
            uploadtask = SwiftUploadQueueTask(
                self.__containerName,
                self.__diskName,
                stream,
                self)
            self.__uploadQueue.put(uploadtask)
            offset += segment_size

        # We need to dump self.__segmentFutures, because if no segment were uploaded
        # for now, second run with resuming upload will cause and empty segments list
        with open(self.__containerName + '.' + self.__diskName + '.txt', 'w') as file:
            json.dump(self.__segmentFutures, file)

        # Creating working threads
        self.__workThreads = []
        file_lock = threading.Lock()
        for i in range(upload_threads):
            # The last connection self.__swiftConnection needed to confirm purposes
            self.__swiftConnection = swiftclient.client.Connection(
                server_url,
                tennant_name + ':' + user_name,
                password,
                retries,
                auth_version='2',
                snet=False,
                ssl_compression=compression)

            thread = SwiftUploadThread(self.__swiftConnection, self.__uploadQueue, file_lock, self.__resumeUpload)
            thread.start()
            self.__workThreads.append(thread)

        super(SwiftUploadChannel_new, self).__init__()

    def uploadData(self, extent):
        try:
            segment_name = '%08d' % int(extent.getStart() / self.__segmentSize)
            stream = DefferedUploadDataStream.getStream(segment_name)
            new_extent = DataExtent.DataExtent(extent.getStart() % self.__segmentSize, extent.getSize())
            new_extent.setData(extent.getData())
            stream.writeData(new_extent)
        except Exception as err:
            pass

        return True

    def getSegmentResults(self):
        return self.__segmentFutures
 
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
            res['action'] = 'put_container'
            self.__swiftConnection.put_container(res['container'], headers=res['headers'], response_dict=resp_dict)

            res['action'] = 'post_container'
            res['headers'] = {'X-Container-Read': '.r:*'}
            self.__swiftConnection.post_container(res['container'], headers=res['headers'], response_dict=resp_dict)

            res['success'] = True
        except (ClientException, Exception) as err:
            logging.error("!!!ERROR: " + err.message)

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
        self.__uploadQueue.join()
        return

    def confirm(self):
        """
        Confirms good upload
        """
        storage_url = None
        try:
            self.__segmentFutures.sort(key=lambda di: di['index'])

            manifest_data = json.dumps([{
                    'path': self.__containerName + '/' + d['path'],
                    'etag': d['etag'],
                    'size_bytes': d['size']
            } for d in self.__segmentFutures
            ])

            mr = {}
            self.__swiftConnection.put_object(
                self.__containerName,
                self.__diskName,
                manifest_data,
                headers={'x-static-large-object': 'true'},
                query_string='multipart-manifest=put',
                response_dict=mr
            )
            storage_url = self.__swiftConnection.url + '/' + self.__containerName + '/' + self.__diskName
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
            for name in DefferedUploadDataStream.opened_streams:
                completed_size += DefferedUploadDataStream.getStream(name).getCompletedSize()
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
        for thread in self.__workThreads:
            self.__uploadQueue.put(None)