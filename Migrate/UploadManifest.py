import uuid
import datetime
import os
import logging

from tinydb import TinyDB, Query
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware


class ImageManifest(object):
    def __init__(self):
        pass

    def insert(self, etag, local_hash, part_name, offset, size, status):
        raise NotImplementedError

    def select(self, etag, part_name):
        raise NotImplementedError

    def update(self, local_hash, part_name, rec):
        raise NotImplementedError

    def all(self):
        raise NotImplementedError

    def get_timestamp(self):
        raise NotImplementedError


class ImageFileManifest(ImageManifest):
    def __init__(self, manifest_path, timestamp, lock, db_write_cache_size=1):
        table = str(timestamp)
        path = "{}/{}".format(manifest_path, table)

        logging.debug(
            "Creating or opening image manifest database {}. It's uses tinydb project "
            "(https://pypi.python.org/pypi/tinydb) and has json format. Just copy-past content of this file "
            "to online web service, that can represent it in more user-friendly format, for example, "
            "http://jsonviewer.stack.hu/ http://www.jsoneditoronline.org/ http://codebeautify.org/jsonviewer"
            .format(path))

        self.__db = None
        self.__table = None
        try:
            # CachingMiddleware: Improves speed by reducing disk I/O. It caches all read operations and writes data
            # to disk every CachingMiddleware.WRITE_CACHE_SIZE write operations.
            cache = CachingMiddleware
            cache.WRITE_CACHE_SIZE = 1 #db_write_cache_size

            self.__db = TinyDB(path, storage=cache(JSONStorage))
            # Creating new table for chunks
            self.__table = self.__db.table(table)
        except Exception as e:
            logging.error("!!!ERROR: Failed to create or open image manifest database: {}".format(e))
            raise

        self.__timestamp = timestamp
        self.__lock = lock

        super(ImageFileManifest, self).__init__()

    def insert_db_meta(self, res):
        with self.__lock:
            self.__db.insert(res)

    def update(self, local_hash, offset, rec):
        """
        Update record in database. Pair (local_hash, offset) have table unique key semantics,
        so in given table there are no records with same hash and part name.

        :param local_hash: etag to search
        :type local_hash: string

        :param offset: data chunk offset
        :type offset: int

        :param rec: dictionary with new values for given record
        :type rec: dict
        """

        with self.__lock:
            key = Query()

            return self.__table.update(rec, key.etag == str(local_hash) and key.offset == str(offset))

    def select(self, etag=None, part_name=None):
        """
        Search records

        :param etag: etag to search
        :type etag: string

        :param part_name: part name to search
        :type part_name: string

        :return: list of records
        """
        key = Query()

        if etag and part_name:
            key = (key.etag == str(etag)) & (key.part_name == str(part_name))
        elif etag:
            key = key.etag == str(etag)
        elif part_name:
            key = key.part_name == str(part_name)

        return self.__table.search(key)

    def insert(self, etag, local_hash, part_name, offset, size, status):
        res = {
            "uuid": str(uuid.uuid4()),
            "etag": str(etag),
            "local_hash": str(local_hash),
            "part_name": str(part_name),
            "offset": str(offset),
            "size": str(size),
            "status": str(status)
        }

        # # We can't insert record with same etag (has unique key semantic)
        # if not self.select(res["etag"], res["part_name"]):
        with self.__lock:
            self.__table.insert(res)

    def all(self):
        with self.__lock:
            return self.__table.all()

    def get_timestamp(self):
        return self.__timestamp


class ImageManifestDatabase(object):
    """
    A class wrapper for tinydb https://pypi.python.org/pypi/tinydb
    for creating and managing image manifest files which allows resuming upload
    """

    def __init__(self, manifest_path, container_name, lock, resume=False, increment_depth=1, db_write_cache_size=1):
        """
        Creates or opens existing manifest file

        :param manifest_path: path to manifest files
        :type manifest_path: string

        :param container_name: image name
        :type container_name: string

        :param lock: synchronization for write (read) to database
        :type lock: threading.Lock()

        :param resume: resuming upload if True
        :type resume: bool

        :param increment_depth: how many backup manifests we should use for incremental backup
        :type increment_depth: int

        :param db_write_cache_size: how many records awaits till they would be written to database
        :type db_write_cache_size: int
        """

        self.__lock = lock
        self.__increment_depth = increment_depth

        # Creating directory if it doesn't exsists
        self.__manifest_path = manifest_path
        if not os.path.isdir(self.__manifest_path):
            os.makedirs(self.__manifest_path)

        self.__db = []
        try:
            # increment_depth = 0 meaning that we use all available manifests
            # increment_depth = N meaning that we use last N (by timestamp) available manifests

            # First, creating manifest if no resume required, than getting list (new manifest just created)
            if resume is False:
                self.__create_manifest(db_write_cache_size)

            m_list = self.__get_sorted_manifest_list(increment_depth)
            for i in m_list:
                self.__db.append(self.__open_manifest(i, db_write_cache_size))

            # Inserting metadata to default table for opened (last) manifest
            self.__db[0].insert_db_meta({
                "start": str(datetime.datetime.now()),
                "container_name": container_name,
                "status": "progress",
                "resume": str(resume),
                "increment_depth": str(increment_depth)})
        except Exception as e:
            logging.error("!!!ERROR: unable to create (or open) image file manifest for {}: {}".format(
                manifest_path, e))
            raise

    def __create_manifest(self, db_write_cache_size=1):
        return ImageFileManifest(
                self.__manifest_path,
                datetime.datetime.now().strftime("%Y-%m-%d %H-%M"),
                self.__lock,
                db_write_cache_size)

    def __open_manifest(self, timestamp, db_write_cache_size=1):
        return ImageFileManifest(
                self.__manifest_path,
                timestamp,
                self.__lock,
                db_write_cache_size)

    def __get_sorted_manifest_list(self, number):
        # Getting list all available manifests (backups) in manifest path, after sorting:
        # f[0] last (by timestamp) manifest
        # f[1...N] previous manifests ordered by timestamp too.
        f = []
        for filename in os.listdir(self.__manifest_path):
            f.append(filename)
        f.sort(reverse=True)

        # len(f) > number > 0 means that if number is less than available manifests in path or 0, we should
        # return whole available list
        return f[0:number] if len(f) > number > 0 else f

    def insert(self, etag, local_hash, part_name, offset, size, status):
        # Inserting in first (meaning last) manifest in list
        return self.__db[0].insert(etag, local_hash, part_name, offset, size, status)

    def select(self, etag=None, part_name=None):
        # TODO: make expression more python-like
        for i in self.__db:
            rec = i.select(etag, part_name)
            if rec:
                return rec

        return []

    def update(self, local_hash, part_name, rec):
        return self.__db[0].update(local_hash, part_name, rec)

    def all(self):
        return self.__db[0].all()

    def get_timestamp(self):
        return self.__db[0].get_timestamp()

    def get_increment_depth(self):
        return self.__increment_depth

    def complete_manifest(self):
        try:
            self.__db[0].insert_db_meta({"end": str(datetime.datetime.now()), "status": "finished"})
        except Exception as e:
            logging.debug("Failed to finalize image manifest file: {}".format(e))

