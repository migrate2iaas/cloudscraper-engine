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

    def get_table_name(self):
        raise NotImplementedError

    def get_path(self):
        raise NotImplementedError


class TinyDBImageFileManifest(ImageManifest):
    """
    Implementation for ImageManifest interface, it's implements database behavior with select, update, insert,...
    for file storage, based on JSON format.
    """

    def __init__(self, manifest_path, timestamp, lock, db_write_cache_size=1, use_dr=False):

        # Change
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

    DB_TABLES_EXTENSION = "!.cloudscraper-manifest-tables"

    def __init__(self, manifest_path, table_name, lock, db_write_cache_size=1, use_dr=False):
        self.__table_name = str(table_name)
        path = "{0}/{1}".format(manifest_path, "{0}{1}".format(table_name, self.DB_TABLES_EXTENSION))

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
            table_name: {},
        }

        if use_dr:
            with open("{0}/{1}".format(manifest_path, file_name), "w") as f:
                json.dump(storage, f)

        return ImageDictionaryManifest.open(
            manifest_path,
            table_name,
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
            with open(self.get_path(), "w") as f:
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
            self.flush()
            self.__db_count += 1

    def update(self, etag, offset, rec):
        """
        """
        for i in self.__table:
            if self.__table[i]["etag"] == str(etag) and self.__table[i]["offset"] == str(offset):
                with self.__lock:
                    self.__table[i] = rec

    def select(self, etag=None, part_name=None, offset=None):
        """
        Search records by given etag or (and) part name

        :param etag: etag to search
        :type etag: string

        :param part_name: part name to search
        :type part_name: string

        param offset: offset to search
        :type offset: string

        :return: list of records or empty []
        """

        res = {}
        for item in self.all():
            if etag and str(offset):
                if item["etag"] == str(etag) and item["offset"] == str(offset):
                    return item
            elif part_name:
                if item["part_name"] == str(part_name):
                    return item
            elif offset:
                if item["offset"] == str(offset):
                    return item

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
            "status": str(status),
        }

        # First check etag for current offset
        rec_offset = self.select(offset=res["offset"])
        if rec_offset:
            # If etag mismatch update record in current manifest for given offset
            if rec_offset["etag"] != res["etag"]:
                res["status"] = "updated"
                self.update(rec_offset["etag"], rec_offset["offset"], res)

        # We can't insert record with same etag and offset (has unique key semantic)
        if not rec_offset:
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

    def get_table_name(self):
        """
        Returns name of backup, in this case it's means manifest file name.

        :return: table (file) name
        """
        return self.__table_name

    def get_path(self):
        if self.__use_dr:
            return "{0}/{1}{2}".format(self.__manifest_path, self.__table_name, self.DB_TABLES_EXTENSION)


class ImageManifestDatabase(object):
    """
    A class for creating and managing image manifest files which allows resuming and incrementing upload
    """

    DB_SCHEME_EXTENSION = ".cloudscraper-manifest-database"

    def __init__(
            self, image_manifest, manifest_path, table_name, lock, increment_depth=1, db_write_cache_size=1,
            use_dr=False, resume=False, volname=None, target_id=None):
        """
        Creates or opens existing manifest file

        :param manifest_path: path to manifest files
        :type manifest_path: string

        :param table_name: table name
        :type table_name: string

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
        self.__target_id = target_id
        self.__volname = str(volname).rsplit('\\').pop().split(':')[0]
        self.__resume = resume
        self.__use_dr = use_dr

        if self.__use_dr:
            self.__increment_depth = increment_depth

            # Creating directory if it doesn't exsists
            self.__manifest_path = manifest_path
            if not os.path.isdir(self.__manifest_path):
                os.makedirs(self.__manifest_path)

        self.__db = []
        self.__db_scheme = None
        try:
            table_name_tmp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
            # If table_name defined in parameter, override it
            if table_name:
                table_name_tmp = table_name

            if self.__use_dr:
                # Creating or opening database scheme
                self.__db_scheme = TinyDB(self.get_db_scheme_path())

                # First, creating manifest if no resume required, then getting list (new manifest just created)
                if self.__resume is False:
                    image_manifest.create(self.__manifest_path, table_name_tmp, lock, db_write_cache_size, use_dr)

                    # Inserting new table name if it's doesn't exists
                    self.__db_scheme.insert({
                        "table_name": str(table_name_tmp),
                        "table_timestamp": str(datetime.datetime.now()),
                        "volname": str(self.__volname),
                        "targed_id": str(self.__target_id),
                    })

                # increment_depth = 0 meaning that we use all available manifests
                # increment_depth = N meaning that we use last N available manifests
                m_list = self.get_db_tables_source(increment_depth, volname, target_id)
                if not m_list and self.__resume:
                    raise Exception("Unable to resuming upload. Previous upload (manifest) not found")

                for table in m_list:
                    self.__db.append(
                        self.__image_manifest.open(
                            self.__manifest_path, table, lock, db_write_cache_size, self.__use_dr))
            else:
                self.__db.append(
                    self.__image_manifest.open(
                        self.__manifest_path, "in_memory_table", lock, db_write_cache_size, self.__use_dr))

            # Saving current manifest table name
            self.__table_name = self.__db[0].get_table_name()

            if self.__use_dr:
                # Updating creation time of current manifest
                self.__db_scheme.update(
                    {"table_timestamp": str(datetime.datetime.now())}, where("table_name") == self.__table_name)

            # Inserting metadata to default table for opened (last) manifest
            self.__db[0].insert_db_meta({
                "start": str(datetime.datetime.now()),
                "table_name": self.__table_name,
                "status": "progress",
                "resume": str(self.__resume),
                "increment_depth": str(increment_depth)})
        except Exception as e:
            logging.error("!!!ERROR: unable to create (or open) image file manifest for {0}: {1}".format(
                manifest_path, e))
            raise

    def get_timestamp(self):
        return self.__db_scheme.get(where("table_name") == str(self.__table_name))["table_timestamp"]

    def get_table_name(self):
        return self.__table_name

    def get_target_id(self):
        return self.__target_id

    def get_increment_depth(self):
        return self.__increment_depth

    def get_db_tables_source(self, increment_depth, volname, targed_id):
        result = []
        t_list = self.__db_scheme.search(where("volname") == str(volname) and where("targed_id") == targed_id)

        # Check, if table_timestamp exists for backward compatibility
        t_list.sort(key=lambda x: x["table_timestamp"] if "table_timestamp" in x else None, reverse=True)
        for table in t_list:
            if "table_timestamp" in table:
                # Adding file extension
                result.append(table["table_name"])

        return result[0:increment_depth] if len(result) > increment_depth > 0 else result

    def get_db_tables(self):
        return self.__db

    def get_db_scheme_path(self):
        return "{0}/{1}".format(self.__manifest_path, self.DB_SCHEME_EXTENSION)

    def get_path(self):
        return self.__manifest_path

    def is_resume(self):
        return self.__resume

    def is_dr(self):
        return self.__use_dr

    def get_key_base(self):
        chainid = self.__table_name
        if self.__increment_depth == 1:
            chainid = self.__table_name + "_full"

        return "{0}/{1}/{2}".format(self.__target_id, chainid, self.__volname)

    def get_part_name(self, offset):
        return "{0}/{1:032}".format(self.get_key_base(), offset)

    def insert(self, etag, local_hash, offset, size, status, part_name=None):
        # inserting in current manifest
        if part_name is None:
            part_name = self.get_part_name(offset)

        return self.__db[0].insert(etag, local_hash, part_name, offset, size, status)

    def select(self, etag=None, part_name=None, offset=None):
        # TODO: make expression more python-like
        for table in self.__db:
            rec = table.select(etag, part_name, offset)
            if rec:
                return rec

        return {}

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

