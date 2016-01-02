import json
import uuid
import datetime
import os

#
#
# class ManifestRecord:
#     def __init__(self, r_uuid=None, name=None, r_md5=None, timestamp=None, offset=None, flags=None, size=None):
#         """
#         """
#         self.__r_uuid = r_uuid
#         self.__name = name
#         self.__r_md5 = r_md5
#         self.__timestamp = timestamp
#         self.__offset = offset
#         self.__flags = flags
#         self.__size = size
#
#     def to_json(self):
#         return "{{\"uuid\": \"{}\", {{\"name\": \"{}\", {{\"md5\": \"{}\", \"timestamp\": \"{}\"," \
#                " \"offset\": \"{}\", \"flags\": \"{}\", \"size\": \"{}\"}}".format(
#                 str(self.__r_uuid),
#                 str(self.__name),
#                 str(self.__r_md5),
#                 str(self.__timestamp),
#                 str("{:032x}").format(self.__offset),
#                 str("{:02x}").format(self.__flags),
#                 str("{:016x}").format(self.__size))
#
#     def from_json(self, r_json):
#         try:
#             rec = json.loads(r_json)
#
#             self.__r_uuid = rec["uuid"]
#             self.__name = rec["name"]
#             self.__r_md5 = rec["md5"]
#             self.__timestamp = rec["flags"]
#             self.__offset = rec["timestamp"]
#             self.__flags = rec["offset"]
#             self.__size = rec["size"]
#         except Exception:
#             raise
#
#     def get_md5(self):
#         return self.__r_md5


class ImageManifestDatabase(object):
    def __init__(self, manifest_path, disk_name, lock):
        self.__manifest_path = manifest_path
        self.__disk_name = disk_name
        self.__lock = lock

    def createManifest(self):
        return ImageFileManifest(
                self.__manifest_path,
                datetime.datetime.now().strftime("%Y-%m-%d %H-%M"),
                self.__disk_name,
                self.__lock)

    def getLastManifest(self):
        f = []
        for filename in os.listdir(self.__manifest_path):
            f.append(filename)
        f.sort(reverse=True)

        return ImageFileManifest(
                self.__manifest_path,
                f[0],
                self.__disk_name,
                self.__lock) if f else None

class ImageManifest(object):
    def __init__(self):
        pass

    def insert(self, etag, part_name, offset, size, status):
        raise NotImplementedError

    def select(self, r_hash):
        raise NotImplementedError

    def update(self, rec):
        raise NotImplementedError

    def dump(self):
        raise NotImplementedError

    def get_path(self):
        raise NotImplementedError

class ImageFileManifest(ImageManifest):
    def __init__(self, manifest_path, timestamp, disk_name, lock):
        self.__source = open("{}/{}".format(manifest_path, timestamp), "a+")
        self.__timestamp = timestamp
        self.__disk_name = disk_name
        self.__lock = lock

        super(ImageFileManifest, self).__init__()

    def insert(self, etag, part_name, offset, size, status):
        if self.select(etag) is None:
            with self.__lock:
                self.__source.write(
                    "{{\"uuid\": \"{}\", \"path\": \"{}\", \"etag\": \"{}\", \"timestamp\": \"{}\", "
                    "\"part_name\": \"{}\", \"offset\": \"{}\", \"size\": \"{:016}\", \"status\": \"{}\"}}\n".format(
                        uuid.uuid4(),
                        self.__disk_name,
                        etag,
                        self.__timestamp,
                        part_name,
                        offset,
                        size,
                        status))
                self.__source.flush()

    def update(self, rec):
        pass

    def select(self, etag):
        with self.__lock:
            self.__source.seek(0)
            for rec in self.__source:
                rec_dict = json.loads(rec)
                if rec_dict["etag"] == etag:
                    return rec_dict

        return None

    def dump(self):
        r_list = []
        with self.__lock:
            self.__source.seek(0)
            for rec in self.__source:
                r_list.append(json.loads(rec))

        return r_list
