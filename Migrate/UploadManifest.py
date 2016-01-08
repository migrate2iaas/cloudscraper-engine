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

    def __init__(self, manifest_path, disk_name, lock, resume=False):
        """
        Creates or opens existing manifest file

        :param manifest_path: path to manifest files
        :type manifest_path: string

        :param disk_name: image name
        :type disk_name: string

        :param lock: synchronization for write (read) to database
        :type lock: threading.Lock()
        """

        self.__disk_name = disk_name
        self.__lock = lock

        # Creating directory if it doesn't exsists
        self.__manifest_path = manifest_path
        if not os.path.isdir(self.__manifest_path):
            os.makedirs(self.__manifest_path)

        # If resuming upload, getting last manifest file (by timestamp), creating new otherwise
        self.__db = None
        try:
            if resume:
                self.__db = self.__get_last_manifet()
            else:
                self.__db = self.__create_manifet()
        except Exception as e:
            logging.error("!!!ERROR: unable to create (or open) image file manifest for {}: {}".format(
                manifest_path, e))
            raise

    def __create_manifet(self):
        return ImageFileManifest(
                self.__manifest_path,
                datetime.datetime.now().strftime("%Y-%m-%d %H-%M"),
                self.__disk_name,
                self.__lock)

    def __get_last_manifet(self):
        f = []
        for filename in os.listdir(self.__manifest_path):
            f.append(filename)
        f.sort(reverse=True)

        return ImageFileManifest(
                self.__manifest_path,
                f[0],
                self.__disk_name,
                self.__lock) if f else None

    def insert(self, etag, local_hash, part_name, offset, size, status):
        return self.__db.insert(etag, local_hash, part_name, offset, size, status)

    def select(self, local_hash):
        return self.__db.select(local_hash)

    def update(self, local_hash, rec):
        return self.__db.update(local_hash, rec)

    def all(self):
        return self.__db.all()

    def get_timestamp(self):
        return self.__db.get_timestamp()



