import json

from lxml import etree

#TODO: split into several modules
# make 'get' virtual
class CloudManifestBuilder(object):

    def __init__(self, cloud, manifest):
        self.__cloud = cloud
        self.__manifest = manifest


class AmazonS3ManifestBuilder(object):

    def __init__(self, connection, manifest, bucket):
        self.__connection = connection
        self.__manifest = manifest
        self.__bucket = bucket
        self.__xml_key = "{0}/{1}".format(self.__manifest.get_key_base(), "!manifest.xml")

    def get(self, linktimeexp_seconds, file_format='VHD'):
        try:
            res_list = self.__manifest.all()

            res_xml = etree.Element('manifest')
            etree.SubElement(res_xml, 'version').text = "2010-11-15"
            etree.SubElement(res_xml, 'file-format').text = str(file_format)

            importer = etree.SubElement(res_xml, 'importer')
            etree.SubElement(importer, 'name').text = "ec2-upload-disk-image"
            etree.SubElement(importer, 'version').text = "1.0.0"
            etree.SubElement(importer, 'release').text = "2010-11-15"

            etree.SubElement(res_xml, 'self-destruct-url').text = self.__connection.generate_url(
                    linktimeexp_seconds,
                    method='DELETE',
                    bucket=self.__bucket,
                    key=self.__xml_key,
                    force_http=False)

            imports = etree.SubElement(res_xml, 'import')
            vol_size_to_allocate = etree.SubElement(imports, 'volume-size')
            parts = etree.SubElement(imports, 'parts', count=str(len(res_list)))

            size = 0
            # sort by offset before passing next to xml build
            res_list = sorted(res_list , key = lambda res: res["offset"])
            for i in res_list:
                size += int(i["size"])

                part = etree.SubElement(parts, 'part', index=str(res_list.index(i)))
                etree.SubElement(
                    part, 'byte-range', end=str(int(i["offset"]) + int(i["size"])), start=str(int(i["offset"])))
                etree.SubElement(part, 'key').text = i["part_name"]

                etree.SubElement(part, 'head-url').text = self.__connection.generate_url(
                    linktimeexp_seconds,
                    method='HEAD',
                    bucket=self.__bucket,
                    key=i["part_name"],
                    force_http=False)

                etree.SubElement(part, 'get-url').text = self.__connection.generate_url(
                    linktimeexp_seconds,
                    method='GET',
                    bucket=self.__bucket,
                    key=i["part_name"],
                    force_http=False)

                etree.SubElement(part, 'delete-url').text = self.__connection.generate_url(
                    linktimeexp_seconds,
                    method='DELETE',
                    bucket=self.__bucket,
                    key=i["part_name"],
                    force_http=False)

            etree.SubElement(imports, 'size').text = str(size)

            gigabyte = 1024 * 1024 * 1024
            vol_size_to_allocate.text = str((size + gigabyte - 1) / gigabyte)

            return "{0}\n{1}".format(
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                etree.tostring(res_xml, pretty_print=True))
        except Exception as e:
            raise Exception("Unable to create xml manifest for Amazon S3, reason: {0}".format(e))

    def get_xml_key(self):
        return self.__xml_key


class OpenStackManifestBuilder(object):
    def __init__(self, manifest, container):
        self.__manifest = manifest
        self.__container = container

    def get(self, swift_use_slo):
        try:
            res_list = self.__manifest.all()

            # Segments can upload not in sequential order, so we need to sort them for manifest
            res_list.sort(key=lambda di: int(di["offset"]))

            # Creating manifest
            manifest_data = json.dumps([{
                    "path": self.__container + "/" + d["part_name"],
                    "etag": d["etag"],
                    "size_bytes": int(d["size"])
                } for d in res_list])

            headers = {"x-static-large-object": "true"}
            query_string = "multipart-manifest=put"

            # For DLO the manifest file is a zero-byte file with the extra X-Object-Manifest {container}/{prefix}
            # header, where {container} is the container the object segments are in and {prefix} is the common prefix
            # for all the segments.
            if not swift_use_slo:
                headers = {"X-Object-Manifest": "{0}/{1}/".format(self.__container, self.__manifest.get_key_base())}
                manifest_data = None
                query_string = None

            return manifest_data, query_string, headers
        except Exception as e:
            raise Exception("Unable to create manifest for OpenStack Swift, reason: {0}".format(e))
