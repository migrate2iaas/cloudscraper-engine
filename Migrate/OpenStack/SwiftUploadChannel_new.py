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

    def read(self , len):
        data = self.__data.readData(len , self.__pos)
        self.__pos += len
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
        if pos == self.__size:
            return ""

        while 1:
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

    def writeData(self , extent):
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


class SwiftUploadTask(MultithreadUpoadChannel.UploadTask):
    def __init__(self,
                 container,
                 keyname,
                 offset,
                 size,
                 data_getter,
                 channel,
                 alternative_source_bucket=None,
                 alternative_source_keyname=None):
        self.__targetContainer = container
        self.__targetKeyname = keyname
        self.__targetSize = size
        self.__dataGetter = data_getter 
        self.__targetOffset = offset
        self.__alternativeKey = alternative_source_keyname
        self.__alternativeBucket = alternative_source_bucket
        
        super(SwiftUploadTask, self).__init__(channel, size, offset)


    def getTargetKey(self):
        return self.__targetKeyname


    def getTargetContainer(self):
        return self.__targetContainer


    def getData(self):
        return str(self.__dataGetter)[0:self.__targetSize]


    def getDataMd5(self):
        md5encoder = md5()
        data = self.getData()
        md5encoder.update(data)
        return md5encoder.hexdigest()


    # specifies the alternitive availble path. 
    # alternative source seem to be interesting concept. get something from another place. maybe deferred
    def isAlternitiveAvailable(self):
        return False
        #return self.__alternativeKey != None and self.__alternativeBucket != None


    #TODO: find more generic way
    # sometimes source could be active too
    def setAlternativeUploadPath(self, alternative_source_bucket = None, alternative_source_keyname = None):
        return None


    #some dedup staff
    def getAlternativeKeyName(self):
        return ""


    def getAlternativeContainer(self):
        return ""


    def getAlternativeKey(self):
        """gets alternative key object"""
        return ""
        #return self.__targetBucket.get_key(self.__alternativeKey)


class SwiftUploadChannel_new(MultithreadUpoadChannel.MultithreadUpoadChannel):
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
                 chunksize=10*1024*1024,
                 upload_threads=10,
                 queue_size=16):
        """constructor"""
        self.__chunkSize = 512*1024#chunksize
        self.__accountName = username
        self.__containerName = container_name
        self.__diskName = disk_name
        self.__accessKey = password
        self.__resumeUpload = resume_upload
        self.__uploadThreads = upload_threads
        self.__serverUrl = server_url
        self.__diskSize = resulting_size_bytes
        self.__sslCompression = compression
        self.__segmentFutures = []

        # self.__nullData = bytearray(self.__chunkSize)
        # md5encoder = md5()
        # md5encoder.update(self.__nullData)
        # self.__nullMd5 = md5encoder.hexdigest()

        # max segment number is 1000 (it's configurable see http://docs.openstack.org/developer/swift/middleware.html )
        self.__segmentSize = 2*1024*1024#2 * self.__chunkSize#max(int(resulting_size_bytes / 512), self.__chunkSize)
        # if self.__segmentSize % self.__chunkSize:
        #     # make segment size an integer of chunks
        #     self.__segmentSize = self.__segmentSize - (self.__segmentSize % self.__chunkSize)

        self.__uploadedSize = 0
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

        offset = 0
        while offset < self.__diskSize:
            if self.__diskSize - offset < self.__chunkSize:
                segment_size = chunk_size = self.__diskSize - offset
            else:
                chunk_size = self.__chunkSize
                segment_size = self.__segmentSize
            DefferedUploadDataStream(str(int(offset / self.__segmentSize)), segment_size, chunk_size)
            offset = offset + self.__segmentSize
        self.__uploadSegmentsJob = threading.Thread(target = self.uploadSegment, args = (container_name, disk_name))
        self.__uploadSegmentsJob.start()

        super(SwiftUploadChannel_new, self).__init__(resulting_size_bytes, resume_upload, chunksize, upload_threads, queue_size)

 
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
        except ClientException as err:
            logging.error("!!!ERROR: " + err.message)
        except Exception as err:
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

    def createUploadTask(self, extent):
        """ 
        Protected method. Creates upload task to add it the queue 
        Returns UploadTask (or any of inherited class) object

        Args:
            extent: DataExtent - extent of the image to be uploaded. Its size and offset should be integral
                of chunk size. The only exception is last chunk.

        """
        start = extent.getStart()
        size = extent.getSize()
        data = extent.getData()

        segment_name = str(int(start / self.__segmentSize))
        logging.debug("Creating upload task for segment '" + segment_name + "' offset:" +
                      str(start) + " and size "+str(size) + " chunk: " + str(self.getTransferChunkSize()))

        return SwiftUploadTask(self.__containerName, segment_name, start, size, data, self)

    def uploadChunk(self, uploadtask):
        """
        Protected. Uploads one chunk of data
        Called by the worker thread internally.
        Could throw any non-recoverable errors
        """
        offset = uploadtask.getUploadOffset() % self.__segmentSize
        size = uploadtask.getUploadSize()
        data = uploadtask.getData()

        extent = DataExtent.DataExtent(offset, size)
        extent.setData(data)
        DefferedUploadDataStream.getStream(uploadtask.getTargetKey()).writeData(extent)
        uploadtask.notifyDataTransfered()
        # container_name = uploadtask.getTargetContainer()
        # disk_name = uploadtask.getTargetKey()
        #
        #
        # chunk_range = 'bytes={}-{}'.format(offset, offset + size - 1)
        #
        #
        # try:
        #     headers = self.__swiftConnection.head_object(container_name, segment_name)
        #     if headers['etag'] == uploadtask.getDataMd5():
        #         headers['segment_location'] = '/' + str(container_name) + '/' + segment_name
        #         headers['segment_etag'] = headers['etag']
        #         headers['segment_size'] = headers['content-length']
        #         headers['segment_offset'] = offset
        #         self.__segmentFutures.append(headers)
        #         logging.info("%s/%s range %s skipped", container_name, disk_name, chunk_range)
        #         uploadtask.notifyDataSkipped()
        #     else:
        #         res = self.uploadSegment(self.__containerName, segment_name, uploadtask)
        #         res['segment_offset'] = offset
        #         self.__segmentFutures.append(res)
        #         logging.info("%s/%s range %s uploaded", str(container_name), disk_name, chunk_range)
        #         uploadtask.notifyDataTransfered()
        # except ClientException as err:
        #     if err.http_status == 404:
        #         try:
        #             res = self.uploadSegment(self.__containerName, segment_name, uploadtask)
        #             res['segment_offset'] = offset
        #             logging.info("%s/%s range %s uploaded", str(container_name), disk_name, chunk_range)
        #             uploadtask.notifyDataTransfered()
        #         except ClientException:
        #             raise
        #     else:
        #         logging.warning("!Failed to upload data: %s/%s range %s", str(container_name), disk_name, chunk_range)
        #         logging.warning("Exception = " + err.message)
        #         logging.error(traceback.format_exc())
        #        raise

        return True


    def uploadSegment(self, container, disk_name):
        """
        """
        for stream in DefferedUploadDataStream.opened_streams:
            segment = DefferedUploadDataStream.getFileProxy(stream)
            segment_name = '%s/slo/%08d' % (disk_name, int(stream))

            results_dict = {}
            res = {
                'success': False,
                'action': 'upload_segment',
                'segment_size': segment.size(),
                'segment_index': int(stream),
                'segment_location': '/%s/%s' % (container, segment_name)}
            try:
                etag = self.__swiftConnection.put_object(container,
                                                         segment_name,
                                                         segment,
                                                         chunk_size=segment.chunk_size(),
                                                         response_dict=results_dict)

                # if etag != uploadtask.getDataMd5():
                #     raise ClientException('Segment {0}: upload verification failed: '
                #                           'md5 mismatch, local {1} != remote {2} '
                #                           '(remote segment has not been removed)'
                #                           .format(segment_name, uploadtask.getDataMd5(), etag))

                res.update({
                    'success': True,
                    'response_dict': results_dict,
                    'segment_etag': etag,
                    'attempts': self.__swiftConnection.attempts
                })
                self.__segmentFutures.append(res)

            except ClientException as err:
                logging.error("!!!ERROR: " + err.message)
                res.update({'success': False})

        return res

    def confirm(self):
        """
        Confirms good upload
        """
        self.__uploadSegmentsJob.join()

        if self.unsuccessfulUpload():
            logging.error("!!!ERROR: there were upload failures. Please, try again by choosing resume upload option!")
            return None

        options = {'verbose': 2}
        options = dict(self.__serviceOpts, **options)
        account_stat = {
            'action': 'stat_account',
            'success': True,
        }
        try:
            self.__segmentFutures.sort(key=lambda di: di['segment_index'])

            manifest_data = json.dumps([{
                    'path': d['segment_location'],
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


    def getDiskUploadedProperty(self):
        """
        Returns amount of data already uploaded as it saved in the cloud storage
        This data could be loaded from the disk object on cloud side which channel represents
        """
        return 0


    def loadDiskUploadedProperty(self):
        logging.debug("Loading disk properties");
        if self.__preUpload:
            return self.__preUpload
        properties = dict()
        #try:
        #    properties = self.__blobService.get_blob_metadata(self.__containerName, self.__diskName);
        #    logging.debug("Got properties: " + properties.__repr__())
        #    self.__preUpload = properties['x-ms-meta-cloudscraper_uploaded'];
        #except Exception as ex:
        #    logging.warning("!Cannot get amount of data already uploaded to the blob. Gonna make full reupload then.")
        #    logging.warning("Exception = " + str(ex)) 
        #    logging.error(traceback.format_exc())
        #    logging.error(repr(properties))
        #    return False
        return True


    def updateDiskUploadedProperty(self , newvalue):
        """
        function to set property to the disk in cloud on the amount of data transfered
        Returns if the update was successful, doesn't throw
        """

        return True    