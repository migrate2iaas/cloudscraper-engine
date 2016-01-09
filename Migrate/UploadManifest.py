import uuid
import datetime
import os
import logging
from tinydb import TinyDB, where


class ImageManifest(object):
    def __init__(self):
        pass

    def insert(self, etag, local_hash, part_name, offset, size, status):
        raise NotImplementedError

    def select(self, local_hash):
        raise NotImplementedError

    def update(self, local_hash, rec):
        raise NotImplementedError

    def all(self):
        raise NotImplementedError

    def get_timestamp(self):
        raise NotImplementedError


class ImageFileManifest(ImageManifest):
    def __init__(self, manifest_path, timestamp, disk_name, lock):
        table = str(timestamp)
        path = "{}/{}".format(manifest_path, table)

        logging.info(">>>>> Creating or opening image manifest database {}. It's uses tinydb project "
                     "(https://pypi.python.org/pypi/tinydb) and has json format. Just copy-past content of this file "
                     "to online web service, that can represent it in more user-friendly format, for example, "
                     "http://jsonviewer.stack.hu/ http://www.jsoneditoronline.org/ http://codebeautify.org/jsonviewer"
                     .format(path))

        self.__db = None
        self.__table = None
        try:
            self.__db = TinyDB(path)
            self.__table = self.__db.table(table)
        except Exception as e:
            logging.error("!!!ERROR: Failed to create or open image manifest database: {}".format(e))
            raise

        self.__timestamp = timestamp
        self.__disk_name = disk_name
        self.__lock = lock

        super(ImageFileManifest, self).__init__()

    def insert(self, etag, local_hash, part_name, offset, size, status):
        res = {
            "uuid": str(uuid.uuid4()),
            "path": str(self.__disk_name),
            "etag": str(etag),
            "local_hash": str(local_hash),
            "part_name": str(part_name),
            "offset": str(offset),
            "size": str(size),
            "status": str(status)
        }

        with self.__lock:
            self.__table.insert(res)

    def update(self, local_hash, rec):
        with self.__lock:
            return self.__table.update(rec, where("local_hash") == str(local_hash))

    def select(self, local_hash):
        # get() returns None if no records in db match condition
        with self.__lock:
            return self.__table.get(where("local_hash") == str(local_hash))

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

    def __init__(self, manifest_path, disk_name, lock, resume=False, increment_depth=1):
        """
        Creates or opens existing manifest file

        :param manifest_path: path to manifest files
        :type manifest_path: string

        :param disk_name: image name
        :type disk_name: string

        :param lock: synchronization for write (read) to database
        :type lock: threading.Lock()

        :param resume: resuming upload if True
        :type resume: bool

        :param increment_depth: how many backup manifests we should use for incremental backup
        :type increment_depth: int
        """

        self.__disk_name = disk_name
        self.__lock = lock

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
                self.__create_manifest()

            m_list = self.__get_sorted_manifest_list(increment_depth)
            for i in m_list:
                self.__db.append(self.__open_manifest(i))
        except Exception as e:
            logging.error("!!!ERROR: unable to create (or open) image file manifest for {}: {}".format(
                manifest_path, e))
            raise

    def __create_manifest(self):
        return ImageFileManifest(
                self.__manifest_path,
                datetime.datetime.now().strftime("%Y-%m-%d %H-%M"),
                self.__disk_name,
                self.__lock)

    def __open_manifest(self, timestamp):
        return ImageFileManifest(
                self.__manifest_path,
                timestamp,
                self.__disk_name,
                self.__lock)

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

    def select(self, local_hash):
        # TODO: make expression more python-like
        for i in self.__db:
            rec = i.select(local_hash)
            if rec:
                return rec

        return None

    def update(self, local_hash, rec):
        return self.__db[0].update(local_hash, rec)

    def all(self):
        return self.__db[0].all()

    def get_timestamp(self):
        return self.__db[0].get_timestamp()



