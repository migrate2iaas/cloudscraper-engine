import uuid
import json
import datetime
import os
import logging

from tinydb import TinyDB, Query, where
from tinydb.storages import JSONStorage, MemoryStorage
from tinydb.middlewares import CachingMiddleware


class ImageManifest(object):
    def __init__(self):
        pass

    @staticmethod
    def create(manifest_path, table_name, lock, db_write_cache_size, use_dr):
        raise NotImplementedError

    @staticmethod
    def open(manifest_path, table_name, lock, db_write_cache_size, use_dr):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def insert(self, etag, local_hash, part_name, offset, size, status):
        raise NotImplementedError

    def select(self, etag=None, part_name=None):
        raise NotImplementedError

    def update(self, etag, offset, rec):
        raise NotImplementedError

    def all(self):
        raise NotImplementedError

    def get_name(self):
        raise NotImplementedError

    def get_path(self):
        raise NotImplementedError


class TinyDBImageFileManifest(ImageManifest):
    """
    Implementation for ImageManifest interface, it's implements database behavior with select, update, insert,...
    for file storage, based on JSON format.
    """

    def __init__(self, manifest_path, timestamp, lock, db_write_cache_size=1, use_dr=False):

        self.__table_name = str(timestamp)
        path = "{0}/{1}".format(manifest_path, self.__table_name)

        logging.debug(
            "Creating or opening image manifest database {0}. It's uses tinydb project "
            "(https://pypi.python.org/pypi/tinydb) and has json format. Just copy-past content of this file "
            "to online web service, that can represent it in more user-friendly format, for example, "
            "http://jsonviewer.stack.hu/ http://www.jsoneditoronline.org/ http://codebeautify.org/jsonviewer"
            .format(path))

        self.__db = None
        self.__table = None
        self.__storage = None
        self.__use_dr = use_dr
        try:
            # CachingMiddleware: Improves speed by reducing disk I/O. It caches all read operations and writes data
            # to disk every CachingMiddleware.WRITE_CACHE_SIZE write operations.
            CachingMiddleware.WRITE_CACHE_SIZE = db_write_cache_size

            self.__storage = CachingMiddleware(JSONStorage)
            self.__db = TinyDB(path, storage=self.__storage)
            # Creating new table for chunks
            self.__table = self.__db.table(self.__table_name)
        except Exception as e:
            logging.error("!!!ERROR: Failed to create or open image manifest database: {0}".format(e))
            raise

        self.__manifest_path = manifest_path
        self.__db_write_cache_size = db_write_cache_size
        self.__lock = lock

        super(TinyDBImageFileManifest, self).__init__()

    @staticmethod
    def create(manifest_path, table_name, lock, db_write_cache_size, use_dr):
        return TinyDBImageFileManifest(
            manifest_path,
            "{0}.cloudscraper-manifest-tables".format(table_name),
            lock,
            db_write_cache_size,
            use_dr)

    @staticmethod
    def open(manifest_path, table_name, lock, db_write_cache_size, use_dr):
        return TinyDBImageFileManifest(
            manifest_path,
            table_name,
            lock,
            db_write_cache_size,
            use_dr)

    def flush(self):
        self.__storage.flush()

    def insert_db_meta(self, res):
        """
        Writes database meta data with given res (dictionary) value. Default meta data table name is "_default"
        and can be found in manifest file.

        :param res: dictionary with meta data
        :type res: dict
        """

        with self.__lock:
            self.__db.insert(res)

    def update(self, etag, offset, rec):
        """
        Update record in database. Pair (etag, offset) has table unique key semantics,
        so in given table there are no records with same hash and offset.

        :param etag: etag to search
        :type etag: string

        :param offset: data chunk offset
        :type offset: int

        :param rec: dictionary with new values for given record
        :type rec: dict
        """

        with self.__lock:
            key = Query()

            return self.__table.update(rec, key.etag == str(etag) and key.offset == str(offset))

    def select(self, etag=None, part_name=None):
        """
        Search records by given etag or (and) part name

        :param etag: etag to search
        :type etag: string

        :param part_name: part name to search
        :type part_name: string

        :return: list of records or empty []
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
        """
        Insert record in database. Pair (etag, offset) has table unique key semantics,
        so in given table there are no records with same hash and offset

        :param etag: etag (hash of remote storage data chunk)
        :type etag: string

        :param local_hash: (hash of local storage data chunk)
        :type local_hash: string

        :param part_name: name for uploaded part
        :type part_name: string

        :param offset: offset of data chunk in whole file
        :type offset: int

        :param size: size of data chunk
        :type size: int

        :param status: dictionary with new values for given record
        :type status: string
        """

        res = {
            "uuid": str(uuid.uuid4()),
            "etag": str(etag),
            "local_hash": str(local_hash),
            "part_name": str(part_name),
            "offset": str(offset),
            "size": str(size),
            "status": str(status)
        }

        # # We can't insert record with same etag and offset (has unique key semantic)
        rec_list = self.select(res["etag"])
        if any(d['offset'] == str(offset) for d in rec_list) is False:
            with self.__lock:
                self.__table.insert(res)

    def all(self):
        """
        Returns all records from current (latest by timestamp)

        :return: list of all records
        """
        with self.__lock:
            return self.__table.all()

    def get_name(self):
        """
        Returns name of backup, in this case it's means manifest file name.

        :return: table (file) name
        """
        return self.__table_name

    def get_path(self):
        if self.__use_dr:
            return "{0}/{1}".format(self.__manifest_path, self.__table_name)


class ImageDictionaryManifest(ImageManifest):
    """
    Implementation for ImageManifest interface, it's implements database behavior with select, update, insert,...
    for file storage, based on JSON format.
    """

    DB_TABLES_EXTENSION = ".cloudscraper-manifest-tables"

    def __init__(self, manifest_path, table_name, lock, db_write_cache_size=1, use_dr=False):
        self.__table_name = str(table_name)
        path = "{0}/{1}".format(manifest_path, self.__table_name)

        logging.debug(
            "Creating or opening image manifest database {0}. It's uses dictionaty to store data."
            .format(path))

        self.__db = {}
        self.__table = {}
        self.__storage = {}
        self.__db_count = 0
        self.__table_count = 0
        self.__use_dr = use_dr

        try:
            if self.__use_dr:
                with open(path) as f:
                    self.__storage = json.load(f)
                self.__db = self.__storage["_default"]
                self.__table = self.__storage[self.__table_name]
            else:
                self.__storage["_default"] = {}
                self.__storage[self.__table_name] = {}
        except Exception as e:
            logging.error("!!!ERROR: Failed to create or open image manifest database: {0}".format(e))
            raise

        self.__manifest_path = manifest_path
        self.__db_write_cache_size = db_write_cache_size
        self.__lock = lock

        super(ImageDictionaryManifest, self).__init__()

    @staticmethod
    def create(manifest_path, table_name, lock, db_write_cache_size, use_dr):
        file_name = "{0}{1}".format(table_name, ImageDictionaryManifest.DB_TABLES_EXTENSION)

        storage = {
            "_default": {},
            file_name: {},
        }

        if use_dr:
            with open("{0}/{1}".format(manifest_path, file_name), "w") as f:
                json.dump(storage, f)

        return ImageDictionaryManifest.open(
            manifest_path,
            file_name,
            lock,
            db_write_cache_size,
            use_dr)

    @staticmethod
    def open(manifest_path, table_name, lock, db_write_cache_size, use_dr):
        return ImageDictionaryManifest(
            manifest_path,
            table_name,
            lock,
            db_write_cache_size,
            use_dr)

    def flush(self):
        if self.__use_dr:
            with open("{0}/{1}".format(self.__manifest_path, self.__table_name), "w") as f:
                json.dump(self.__storage, f)

    def insert_db_meta(self, res):
        """
        Writes database meta data with given res (dictionary) value. Default meta data table name is "_default"
        and can be found in manifest file.

        :param res: dictionary with meta data
        :type res: dict
        """

        with self.__lock:
            self.__db[self.__db_count] = res
            self.__db_count += 1

    def update(self, etag, offset, rec):
        """
        """
        pass

    def select(self, etag=None, part_name=None):
        """
        Search records by given etag or (and) part name

        :param etag: etag to search
        :type etag: string

        :param part_name: part name to search
        :type part_name: string

        :return: list of records or empty []
        """

        res = []
        for item in self.all():
            if etag:
                if item["etag"] == str(etag):
                    if part_name:
                        if item["part_name"] == str(part_name):
                            res.append(item)
                    else:
                        res.append(item)
            elif part_name:
                if item["part_name"] == str(part_name):
                    res.append(item)

        return res

    def insert(self, etag, local_hash, part_name, offset, size, status):
        """
        Insert record in database. Pair (etag, offset) has table unique key semantics,
        so in given table there are no records with same hash and offset

        :param etag: etag (hash of remote storage data chunk)
        :type etag: string

        :param local_hash: (hash of local storage data chunk)
        :type local_hash: string

        :param part_name: name for uploaded part
        :type part_name: string

        :param offset: offset of data chunk in whole file
        :type offset: int

        :param size: size of data chunk
        :type size: int

        :param status: dictionary with new values for given record
        :type status: string
        """

        res = {
            "uuid": str(uuid.uuid4()),
            "etag": str(etag),
            "local_hash": str(local_hash),
            "part_name": str(part_name),
            "offset": str(offset),
            "size": str(size),
            "status": str(status)
        }

        # We can't insert record with same etag and offset (has unique key semantic)
        rec_list = self.select(res["etag"])
        if any(d['offset'] == str(offset) for d in rec_list) is False:
            with self.__lock:
                self.__table[self.__table_count] = res
                self.__table_count += 1

                # If write count matches db_write_cache_size flush database
                if self.__table_count % self.__db_write_cache_size == 0:
                    self.flush()

    def all(self):
        """
        Returns all records from current (latest by timestamp)

        :return: list of all records
        """
        with self.__lock:
            # TODO: make more python-like, we need return dictionary without record number (1, 2, ...)
            res = []
            for i in self.__table:
                res.append(self.__table[i])
            return res

    def get_name(self):
        """
        Returns name of backup, in this case it's means manifest file name.

        :return: table (file) name
        """
        return self.__table_name

    def get_path(self):
        if self.__use_dr:
            return "{0}/{1}".format(self.__manifest_path, self.__table_name)


class ImageManifestDatabase(object):
    """
    A class for creating and managing image manifest files which allows resuming and incrementing upload
    """

    DB_SCHEME_EXTENSION = ".cloudscraper-manifest-database"

    def __init__(
            self, image_manifest, manifest_path, container_name, lock, resume=False, increment_depth=1,
            db_write_cache_size=1, use_dr=False):
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

        self.__increment_depth = None
        self.__manifest_path = None
        self.__image_manifest = image_manifest
        if use_dr:
            self.__increment_depth = increment_depth

            # Creating directory if it doesn't exsists
            self.__manifest_path = manifest_path
            if not os.path.isdir(self.__manifest_path):
                os.makedirs(self.__manifest_path)

        self.__db = []
        self.__db_scheme = None
        try:
            if use_dr:
                # Creating or opening database scheme
                self.__db_scheme = TinyDB(self.get_db_scheme_path())

                # increment_depth = 0 meaning that we use all available manifests
                # increment_depth = N meaning that we use last N (by timestamp) available manifests

                # First, creating manifest if no resume required, than getting list (new manifest just created)
                if resume is False:
                    image_manifest.create(self.__manifest_path, container_name, lock, db_write_cache_size, use_dr)

                m_list = self.get_db_tables_source(increment_depth)
                if not m_list and resume:
                    raise Exception("Unable to resuming upload. Previous upload (manifest) not found")

                for table_name in m_list:
                    self.__db.append(
                        self.__image_manifest.open(
                            self.__manifest_path, table_name, lock, db_write_cache_size, use_dr))
                    # Inserting new table name if it's doesn't exists
                    if not self.__db_scheme.search(where("table_name") == str(table_name)):
                        self.__db_scheme.insert({"table_name": str(table_name)})
            else:
                self.__db.append(
                    self.__image_manifest.open(
                        self.__manifest_path, "in_memory_table", lock, db_write_cache_size, use_dr))

            # Inserting metadata to default table for opened (last) manifest
            self.__db[0].insert_db_meta({
                "start": str(datetime.datetime.now()),
                "container_name": container_name,
                "status": "progress",
                "resume": str(resume),
                "increment_depth": str(increment_depth)})
        except Exception as e:
            logging.error("!!!ERROR: unable to create (or open) image file manifest for {0}: {1}".format(
                manifest_path, e))
            raise

    def get_db_tables_source(self, number):
        number = 0
        # Getting list all available manifests (backups) in manifest path, after sorting:
        # f[0] last (by timestamp) manifest
        # f[1...N] previous manifests ordered by timestamp too.
        f = []
        for filename in os.listdir(self.__manifest_path):
            if filename.endswith(self.__image_manifest.DB_TABLES_EXTENSION):
                f.append(filename)
        f.sort(reverse=True)

        # len(f) > number > 0 means that if number is less than available manifests in path or 0, we should
        # return whole available list
        return f[0:number] if len(f) > number > 0 else f

    def get_db_tables(self):
        return self.__db

    def get_db_scheme_path(self):
        return "{0}/{1}".format(self.__manifest_path, self.DB_SCHEME_EXTENSION)

    def insert(self, etag, local_hash, part_name, offset, size, status):
        # Inserting in first (meaning last) manifest in list
        return self.__db[0].insert(etag, local_hash, part_name, offset, size, status)

    def select(self, etag=None, part_name=None):
        # TODO: make expression more python-like
        for table in self.__db:
            rec = table.select(etag, part_name)
            if rec:
                return rec

        return []

    def update(self, local_hash, part_name, rec):
        return self.__db[0].update(local_hash, part_name, rec)

    def all(self):
        self.__db[0].flush()

        return self.__db[0].all()

    def get_increment_depth(self):
        return self.__increment_depth

    def complete_manifest(self, excpected_image_size):
        try:
            # Calculating sum of all chunk size in current database
            actual_image_size = 0
            for record in self.all():
                actual_image_size += int(record["size"])

            self.__db[0].insert_db_meta({
                "end": str(datetime.datetime.now()),
                "status": "finished",
                "excpected_image_size": str(excpected_image_size),
                "actual_image_size": str(actual_image_size)})

            self.__db[0].flush()

        except Exception as e:
            logging.debug("Failed to finalize image manifest file: {0}".format(e))
            raise


class ImageWellKnownBlockDatabase(object):

    def __init__(self, lock):
        self.__lock = lock
        self.__db = TinyDB(storage=MemoryStorage)

    def insert(self, etag, part_name, data):
        with self.__lock:
            return self.__db.insert({
                "etag": str(etag),
                "part_name": str(part_name),
                "data": str(data)})

    def select(self, etag, data):
        key = Query()
        key = (key.etag == str(etag)) & (key.data == str(data))

        return self.__db.get(key)

    def update(self):
        # Not implemented
        pass

    def all(self):
        return self.__db.all()

    def get_name(self):
        # Not implemented
        pass

    def get_path(self):
        # Not implemented
        pass

