import json


class ManifestRecord:
    def __init__(self, r_uuid=None, name=None, r_md5=None, timestamp=None, offset=None, flags=None, size=None):
        """
        """
        self.__r_uuid = r_uuid
        self.__name = name
        self.__r_md5 = r_md5
        self.__timestamp = timestamp
        self.__offset = offset
        self.__flags = flags
        self.__size = size

    def to_json(self):
        return "{{\"uuid\": \"{}\", {{\"name\": \"{}\", {{\"md5\": \"{}\", \"timestamp\": \"{}\"," \
               " \"offset\": \"{}\", \"flags\": \"{}\", \"size\": \"{}\"}}".format(
                str(self.__r_uuid),
                str(self.__name),
                str(self.__r_md5),
                str(self.__timestamp),
                str("{:032x}").format(self.__offset),
                str("{:02x}").format(self.__flags),
                str("{:016x}").format(self.__size))

    def from_json(self, r_json):
        try:
            rec = json.loads(r_json)

            self.__r_uuid = rec["uuid"]
            self.__name = rec["name"]
            self.__r_md5 = rec["md5"]
            self.__timestamp = rec["flags"]
            self.__offset = rec["timestamp"]
            self.__flags = rec["offset"]
            self.__size = rec["size"]
        except Exception:
            raise

    def get_md5(self):
        return self.__r_md5


class ImageManifest:
    def __init__(self, source, lock):
        pass

    def insert(self, rec):
        raise NotImplementedError

    def select(self, r_md5):
        raise NotImplementedError

    def update(self, rec):
        raise NotImplementedError


class ImageFileManifest(ImageManifest):
    def __init__(self, source, lock):
        self.__source = source
        self.__lock = lock

        super(ImageFileManifest, self).__init__(source, lock)

    def insert(self, rec):
        # Check if md5 is exists
        try:
            self.select(rec.get_md5())
        except KeyError:
            pass
        except Exception:
            raise

        try:
            with self.__lock:
                self.__source.write("{}\n".format(rec.to_json()))
        except Exception:
            raise

    def update(self, rec):
        pass

    def select(self, r_md5):
        try:
            for rec in self.__source:
                if r_md5 == str(ManifestRecord().from_json(rec).get_md5()):
                    return ManifestRecord().from_json(rec)
        except Exception:
            raise

        # If not found:
        # exception KeyError: Raised when a mapping (dictionary) key is not found in the set of existing keys.
        raise KeyError
