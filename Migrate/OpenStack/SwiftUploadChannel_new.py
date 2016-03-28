# --------------------------------------------------------
__author__ = "Alexey Kondratiev"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import sys

sys.path.append('.\..')
sys.path.append('.\..\OpenStack')
sys.path.append('.\OpenStack')

import UploadChannel

import datetime
import Queue
import traceback
import logging
import threading
import swiftclient.client
import UploadChannel
import UploadManifest

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
        self.__skipped_size = 0
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
        if not self.__cancel and not self.__completed.is_set():
            self.__inner_queue.put(extent)

    def getSize(self):
        return self.__size

    def getCompletedSize(self):
        return self.__readed_size

    def getSkippedSize(self):
        return self.__skipped_size

    def setComplete(self, skipped=False):
        """sets the data is uploaded.
        Args:
            skipped (Boolean): set to true then data marked as skipped, not read
        """
        if skipped:
            # When we skipping segment its means that we skipping all data,
            # so skipped size equals segment size
            self.__skipped_size = self.__size
        self.__completed.set()

        # just get one element to avoid deadlocks (works if there is only one writer thread)
        # self.__inner_queue.get_nowait()

    def waitTillComplete(self):
        if not self.cancelled():
            self.__completed.wait()

    def getMD5(self):
        return self.__md5encoder.hexdigest()

    def cancel(self):
        if not self.__cancel:
            with self.__inner_queue.mutex:
                self.__cancel = True
                self.setComplete()

    def cancelled(self):
        return self.__cancel


    def release(self):
        """
            Releasing all resources here.
        """
        # The way to clear all tasks in queue
        while not self.__inner_queue.empty():
            try:
                self.__inner_queue.get(False)
            except Queue.Empty:
                continue
            self.__inner_queue.task_done()


class SwiftUploadThread(threading.Thread):
    """thread making all uploading works"""
    def __init__(self, upload_channel, file_proxy, offset, manifest, ignore_etag=False):
        self.__uploadChannel = upload_channel
        self.__fileProxy = file_proxy
        self.__offset = offset
        self.__manifest = manifest
        self.__ignoreEtag = ignore_etag


        super(SwiftUploadThread, self).__init__()

    def run(self):
        if self.__uploadChannel.skipExisting():
            logging.debug("Upload thread started with reuploading turned on")


        upload = True
        connection = None
        try:
            connection = self.__uploadChannel.createConnection()

            # part name, starts with zeros, 32 character length, needed for dlo, because they use sorted
            # sequence to define segment order.
            part_name = "{0}/{1:032}".format(self.__uploadChannel.getPartPrefix(), self.__offset)

            # Trying to check existing segment
            try:
                # Select returns list of records matches part_name from manifest database
                res = self.__manifest.select(part_name=part_name)
                if res and not self.__ignoreEtag:
                    # Check, if segment with same local part_name exsists in storage, and
                    # etag in manifest and storage are the same
                    for i in res:
                        head = connection.head_object(self.__uploadChannel.getContainerName(), part_name)
                        if i["etag"] == head["etag"]:
                            # We should insert new record if this part found in another manifest
                            self.__manifest.insert(
                                i["etag"], i["local_hash"], part_name, self.__offset, self.__fileProxy.getSize(),
                                "skipped")
                            upload = False
                            logging.info("Data upload skipped for {0}".format(i["part_name"]))
                            self.__uploadChannel.notifyOverallDataSkipped(self.__fileProxy.getSize())

            except (ClientException, Exception) as e:
                # Passing exception here, it"s means that when we unable to check
                # uploaded segment (it"s missing or etag mismatch) we reuploading that segment
                logging.warning("Segment {0} verification failed: {1}".format(part_name, e))
                pass

            if upload:
                etag = connection.put_object(
                    self.__uploadChannel.getContainerName(),
                    part_name,
                    self.__fileProxy,
                    chunk_size=self.__uploadChannel.getChunkSize())

                # getMD5() updates only when data in file proxy (used by put_object()) read.
                segment_md5 = self.__fileProxy.getMD5()
                # TODO: make status ("uploaded") as enumeration
                self.__manifest.insert(
                    etag, segment_md5, part_name, self.__offset, self.__fileProxy.getSize(), "uploaded")
                self.__uploadChannel.notifyOverallDataTransfered(self.__fileProxy.getSize())
        except (ClientException, Exception) as e:
            self.__fileProxy.cancel()
            logging.warn("!Unable to upload segment {0}. Reason: {1}".format(self.__offset, e))
        finally:
            # We should compete every file proxy to avoid deadlocks
            # Notify that upload complete
            self.__fileProxy.setComplete(upload)

            # Each file proxy must be released, because internally it"s use Queue
            # synchronization primitive and it must be released, when, for example, exception happens
            self.__fileProxy.release()

            # Closing swift connection and completing upload thread.
            # Every thread creation must call completeUploadThread() function to avoid
            # uploadThreads semaphore overflowing.
            if connection:
                connection.close()
            self.__uploadChannel.completeUploadThread()

        logging.debug("Upload thread for {0} done".format(self.__offset))


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
            manifest_path=None,
            increment_depth=1,
            chunksize=1024*1024*10,
            upload_threads=10,
            queue_size=8,
            ignore_etag=False,
            swift_use_slo=True,
            swift_max_segments=0,
            use_dr=False):
        """constructor"""
        self.__serverURL = server_url
        self.__userName = username
        self.__tennantName = tennant_name
        self.__password = password
        self.__retries = retries
        self.__compression = compression
        self.__diskName = disk_name
        self.__chunkSize = chunksize
        self.__diskSize = resulting_size_bytes
        self.__resumeUpload = resume_upload
        self.__uploadThreads = threading.BoundedSemaphore(upload_threads)
        self.__segmentQueueSize = queue_size
        self.__swift_use_slo = swift_use_slo
        self.__segmentsList = []

        self.__fileProxies = []
        self.__ignoreEtag = ignore_etag

        # Upload size calculation
        self.__uploadedSize = 0
        self.__uploadSkippedSize = 0
        self.__containerName = container_name

        self.__maxSegments = swift_max_segments
        if swift_max_segments == 0:
            self.__maxSegments = 512

        # Max segment number is 1000 (it"s configurable see http://docs.openstack.org/developer/swift/middleware.html )
        self.__segmentSize = max(int(self.__diskSize / self.__maxSegments), self.__chunkSize)
        if self.__segmentSize % self.__chunkSize:
            # Make segment size an integer of chunks
            self.__segmentSize -= self.__segmentSize % self.__chunkSize

        logging.info("Segment size: " + str(self.__segmentSize) + " chunk size: " + str(self.__chunkSize))
        logging.info("SSL compression is " + str(self.__compression))
        logging.info("Static large objects is " + str(self.__swift_use_slo))

        # Resume upload
        logging.info("Resume upload file path: {0}, resume upload is {1}".format(manifest_path, self.__resumeUpload))
        self.__manifest = None
        try:
            self.__manifest = UploadManifest.ImageManifestDatabase(
                UploadManifest.ImageDictionaryManifest,
                manifest_path,
                '{0}.{1}'.format(self.__containerName, self.__diskName).replace('/', '.'),
                threading.Lock(),
                self.__resumeUpload,
                increment_depth,
                use_dr=use_dr)
            logging.info("Manifest name: {0}".format(self.__manifest.get_table_name()))
        except Exception as e:
            logging.error("!!!ERROR: cannot open file containing segments. Reason: {0}".format(e))
            raise

        super(SwiftUploadChannel_new, self).__init__()


    def uploadData(self, extent):
        logging.debug("Uploading extent: start: " + str(extent.getStart()) + " size: " + str(extent.getSize()))

        index = extent.getStart() / self.__segmentSize
        offset = extent.getStart() % self.__segmentSize

        # If needed to upload new segment
        if offset == 0:
            # Checking if we can create more upload threads, they releases when calls completeUploadThread() routine
            self.__uploadThreads.acquire()
            logging.debug("Starting new upload thread for \"%08d\"" % index)
            # Checking if this is the last segment
            if extent.getStart() + self.__segmentSize > self.__diskSize:
                segment_size = self.__diskSize - extent.getStart()
            else:
                segment_size = self.__segmentSize
            self.__fileProxies.insert(index, DefferedUploadFileProxy(self.__segmentQueueSize, segment_size))

            SwiftUploadThread(
                self,
                self.__fileProxies[index],
                extent.getStart(),
                self.__manifest,
                self.__ignoreEtag).start()

        self.__fileProxies[index].write(extent)

        return True


    def completeUploadThread(self):
        # Releasing semaphore. This call must be for every created upload thread
        # to avoid thread endless waiting.
        self.__uploadThreads.release()


    def getContainerName(self):
        return self.__containerName


    def getDiskName(self):
        return self.__diskName


    def skipExisting(self):
        return self.__resumeUpload


    def getResumePath(self):
        return self.__resumePath


    def getChunkSize(self):
        return self.__chunkSize


    def createConnection(self):
        return swiftclient.client.Connection(
            self.__serverURL,
            self.__tennantName + ":" + self.__userName,
            self.__password,
            self.__retries,
            auth_version="2",
            snet=False,
            ssl_compression=self.__compression,
            timeout=86400)

    def initStorage(self, init_data_link=""):
        """
        Inits storage to run upload
        Throws in case of unrecoverable errors
        """
        connection = self.createConnection()
        connection.put_container(self.__containerName)

        return True

    def __set_container_acl(self, container_name, acl):
        connection = None
        success = True
        try:
            connection = self.createConnection()
            connection.post_container(container_name, {"X-Container-Read": ".r:{0}".format(acl)})

        except (ClientException, Exception) as err:
            logging.error("!!!ERROR: " + err.message)
            success = False
        finally:
            if connection:
                connection.close()

        return success

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
        logging.info("Upload complete, waiting for threads to complete...")
        # Waiting till upload queues in all proxy files becomes empty
        for item in self.__fileProxies:
            item.waitTillComplete()

        return

    def confirm(self):
        """
        Confirms good upload
        """
        logging.info("Confirming good upload")
        storage_url = None
        try:
            # If segments size and disk size mismatch
            total_size = 0
            r_list = self.__manifest.all()
            for rec in r_list:
                total_size += int(rec["size"])

            if total_size != self.__diskSize:
                raise ClientException("Not all segments uploaded successfully: {0} uploaded, {1} expected".format(
                    total_size, self.__diskSize))

            # Segments can upload not in sequential order, so we need to sort them for manifest
            r_list.sort(key=lambda di: int(di["offset"]))
            # Use SLO or not, this call returns different manifest file type
            storage_url = self.__uploadCloudManifest(self.__createCloudManifest(r_list), self.__swift_use_slo)

            # Notify manifest database that backup completed
            self.__manifest.complete_manifest(total_size)
        except (ClientException, Exception) as err:
            logging.error("!!!ERROR: " + err.message)
            logging.error(traceback.format_exc())

        # make container as public storage
        if self.__set_container_acl(self.__containerName, '*'):
            logging.info("Making {0} as public storage".format(self.__containerName))
        else:
            logging.error("!!!ERROR: unable to make {0} as public storage".format(self.__containerName))
            return None

        return storage_url

    def __createCloudManifest(self, segment_list):
        # Creating manifest
        return json.dumps([{
                "path": self.__containerName + "/" + d["part_name"],
                "etag": d["etag"],
                "size_bytes": int(d["size"])
        } for d in segment_list])

    def __uploadCloudManifest(self, manifest_data, swift_use_slo):

        # For DLO the manifest file is a zero-byte file with the extra X-Object-Manifest {container}/{prefix} header,
        # where {container} is the container the object segments are in and {prefix} is the common prefix for all
        # the segments.

        mr = {}
        headers = {"x-static-large-object": "true"}
        query_string = "multipart-manifest=put"
        if not swift_use_slo:
            headers = {"X-Object-Manifest": "{0}/{1}/".format(self.__containerName, self.getPartPrefix())}
            manifest_data = None
            query_string = None

        connection = self.createConnection()
        cloud_path = "{0}/{1}".format(self.__diskName, self.__manifest.get_timestamp())
        connection.put_object(
            self.__containerName,
            cloud_path,
            manifest_data,
            headers=headers,
            query_string=query_string,
            response_dict=mr
        )
        storage_url = connection.url + "/" + self.__containerName + "/" + cloud_path
        connection.close()

        return storage_url

    def getPartPrefix(self):
        """
        Returns prefix for all the parts in this upload session
        """
        base = "{0}/{1}".format(self.getDiskName(), self.__manifest.get_timestamp())
        if self.__swift_use_slo:
            return "{0}/slo".format(base)
        else:
            return "{0}/dlo".format(base)

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


    def notifyOverallDataSkipped(self, size):
        self.__uploadSkippedSize += size


    def getOverallDataSkipped(self):
        """
        Gets overall size of data skipped in bytes.
        Data is skipped by the channel when the block with same checksum is already present in the cloud
        """
        return self.__uploadSkippedSize


    def notifyOverallDataTransfered(self, size):
        self.__uploadedSize += size


    def getOverallDataTransfered(self):
        """
        Gets overall size of data actually uploaded (not skipped) in bytes.
        """
        return self.__uploadedSize


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
        # make container as private storage
        if self.__set_container_acl(self.__containerName, ''):
            logging.info("Making {0} as private storage".format(self.__containerName))
        else:
            logging.warning("Unable to make {0} as private storage".format(self.__containerName))
