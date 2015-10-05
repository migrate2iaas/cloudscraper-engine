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

    def __init__(self , name , size, chunksize, max_part_number=100):
        self.__name = name
        self.__pos = 0
        self.__size = size
        self.__parts = dict()
        self.__chunksize = chunksize
        DefferedUploadDataStream.opened_streams[name] = self
        self.__semaphore = threading.Semaphore(max_part_number)
        self.__dictLock = threading.Lock()
        self.__cancel = False
        self.__completeDataSize = 0

    def readData(self , len , pos):
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
    def __init__(self, connection, container_name, disk_name, stream_proxy, channel):
        self.__swiftConnection = connection
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
    def __init__(self, queue, lock, skip_existing = False):
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
            file_proxy = stream_proxy.getFileProxy(stream_proxy.getName())
            segment_path = '%s/slo/%s' % (upload_task.getDiskName(), stream_proxy.getName())
            connection = upload_task.getSwiftConnection()

            res = {
                'success': False,
                'action': 'upload_segment',
                'segment_size': stream_proxy.getSize(),
                'segment_index': int(stream_proxy.getName()),
                'segment_path': '/%s/%s' % (upload_task.getContainerName(), segment_path)}
            try:
                if self.__skipExisting:
                    seg_res = upload_task.getChannel().getSegmentResults()
                    segment = next((i for i in seg_res if i['segment_path'] == res['segment_path']), None)
                    if segment is not None:
                        headers = connection.head_object(upload_task.getContainerName(), segment_path)
                        if headers['etag'] == segment['segment_etag']:
                            self.__uploadQueue.task_done()
                            continue

                etag = connection.put_object(
                    upload_task.getContainerName(),
                    segment_path,
                    file_proxy,
                    chunk_size=stream_proxy.getChunkSize(),
                    response_dict=results_dict)
                segment_md5 = file_proxy.getMD5()
                if etag != segment_md5:
                    raise ClientException('Segment {0}: upload verification failed: '
                                          'md5 mismatch, local {1} != remote {2} '
                                          '(remote segment has not been removed)'
                                          .format(segment_name, segment_md5, etag))

                res.update({
                    'success': True,
                    'response_dict': results_dict,
                    'segment_etag': etag,
                    'attempts': connection.attempts
                })
                upload_task.getChannel().appendSegmentResult(res)

                # Dumping segment results for resuming upload, if needed
                try:
                    self.__fileLock.acquire()
                    with open(upload_task.getContainerName() + '.' + upload_task.getDiskName() + '.txt', 'w') as file:
                        json.dump(upload_task.getChannel().getSegmentResults(), file)
                finally:
                    self.__fileLock.release()

            except (ClientException, Exception) as err:
                logging.error("!!!ERROR: " + err.message)
                logging.error(traceback.format_exc())
                res.update({'success': False})

            self.__uploadQueue.task_done()

class SwiftUploadChannel_new(UploadChannel.UploadChannel):
    """
    Upload channel for Swift implementation
    Implements multithreaded fie upload to Openstack Swift
    """

    def __init__(self,
                 resulting_size_bytes,
                 server_url,
                 username,
                 tennant_name,
                 password,
                 disk_name, 
                 container_name,
                 compression=False,
                 resume_upload=False,
                 chunk_size=65536,
                 upload_threads=4,
                 queue_size=16):
        """constructor"""
        self.__accountName = username
        self.__containerName = container_name
        self.__diskName = disk_name
        self.__chunkSize = chunk_size
        self.__segmentSize = 20 * self.__chunkSize
        self.__diskSize = resulting_size_bytes
        self.__accessKey = password
        self.__resumeUpload = resume_upload
        self.__uploadThreads = upload_threads
        self.__serverUrl = server_url
        self.__sslCompression = compression
        self.__segmentFutures = []
        #
        # # max segment number is 1000 (it's configurable see http://docs.openstack.org/developer/swift/middleware.html )
        # self.__segmentSize = 2*1024*1024#2 * self.__chunkSize#max(int(resulting_size_bytes / 512), self.__chunkSize)
        # # if self.__segmentSize % self.__chunkSize:
        # #     # make segment size an integer of chunks
        # #     self.__segmentSize = self.__segmentSize - (self.__segmentSize % self.__chunkSize)
        #
        # self.__uploadedSize = 0

        # Creating swift connection
        self.__serviceOpts = {'auth': server_url,
                              'user': tennant_name + ':' + self.__accountName,
                              'key': self.__accessKey,
                              'auth_version': '2',
                              'snet': False,
                              'segment_threads': upload_threads,
                              'segment_size': self.__segmentSize,
                              'ssl_compression': self.__sslCompression}

        self.__serviceOpts = dict(_default_global_options, **dict(_default_local_options, **self.__serviceOpts))
        process_options(self.__serviceOpts)
        self.__swiftConnection = get_conn(self.__serviceOpts)

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
        chunk_size = self.__chunkSize
        while offset < self.__diskSize:
            if self.__diskSize - offset < segment_size:
                segment_size = self.__diskSize - offset
            stream = DefferedUploadDataStream('%08d' % int(offset / self.__segmentSize), segment_size, chunk_size)
            uploadtask = SwiftUploadQueueTask(
                self.__swiftConnection,
                self.__containerName,
                self.__diskName,
                stream,
                self)
            self.__uploadQueue.put(uploadtask)
            offset += segment_size

        # Creating working threads
        self.__workThreads = []
        file_lock = threading.Lock()
        for i in range(self.__uploadThreads):
            thread = SwiftUploadThread(self.__uploadQueue, file_lock, self.__resumeUpload)
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

    def appendSegmentResult(self, segment):
        self.__segmentFutures.append(segment)

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

        options = {'verbose': 2}
        options = dict(self.__serviceOpts, **options)
        account_stat = {
            'action': 'stat_account',
            'success': True,
        }
        try:
            self.__segmentFutures.sort(key=lambda di: di['segment_index'])

            manifest_data = json.dumps([{
                    'path': d['segment_path'],
                    'etag': d['segment_etag'],
                    'size_bytes': d['segment_size']
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

            items, headers = stat_account(self.__swiftConnection, options=options)
            account_stat.update({
                'items': items,
                'headers': headers
            })
            logging.debug("Account stat: " + repr(account_stat))
        except ClientException as err:
            logging.error("!!!ERROR: " + err.message)
            account_stat.update({'success': False})
            return None
        except Exception as err:
            logging.error("!!!ERROR: " + err.message)
            account_stat.update({'success': False})
            return None

        # TODO: refactor code below
        storage_url = ""
        for stat_tuples in account_stat['items']:
            if str(stat_tuples[0]) == "StorageURL":
                storage_url = stat_tuples[1]
                break

        if not storage_url:
            logging.error("!!!ERROR: cannot find storage account url!")
            return None

        if account_stat['headers'].has_key('X-Account-Meta-Temp-URL-Key'):
            key = account_stat['headers']['X-Account-Meta-Temp-URL-Key']
        elif account_stat['headers'].has_key('X-Account-Meta-Temp-URL-Key'.lower()):
            key = account_stat['headers']['X-Account-Meta-Temp-URL-Key'.lower()]
        else:
            key = None
        # else:
        #     logging.info("Account URL Key not found, setting our own one")
        #     key = self.__accessKey
        #     options = {'headers': {'': key}}
        #     self.__swiftService.post(options=options)

        logging.info(repr(account_stat))
        path_url_part = storage_url[storage_url.find("/v1/"):]
        base_url_part = storage_url.replace(path_url_part , "")
        path = path_url_part+"/"+self.__containerName+"/"+self.__diskName
        method = "GET"

        seconds =  24*60*60 # 24 hours
        # dunno but Webzilla takes milliseconds instead of seconds
        milliseconds = int(time.time())*1000 + seconds*1000 - time.time() + seconds
        #milliseconds = 1440535811144 - time.time()
        #key should be set as metdata Temp-URL-Key:
        url = generate_temp_url(path, milliseconds, key, method)
        logging.debug("Swift temp url is " + url)
        # temp url doesn't work if object is not public - glance ignores the URI signature and parms when making request
        return base_url_part+path #+url


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
        if self.__proxyFileObj == None:
            return 0
        return self.__proxyFileObj.getCompletedSize()


    def close(self):
        """
        Closes the channel, deallocates any associated resources
        """
        return


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