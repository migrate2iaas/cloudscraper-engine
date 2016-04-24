from lxml import etree


class CloudManifestBuilder(object):

    def __init__(self, cloud, manifest):
        self.__cloud = cloud
        self.__manifest = manifest


class AmazonS3ManifestBuilder(object):

    def __init__(self, connection, manifest, bucket):
        self.__connection = connection
        self.__manifest = manifest
        self.__bucket = bucket

    def get(self, linktimeexp_seconds, file_format='VHD'):
        res_list = self.__manifest.all()

        res_xml = etree.Element('manifest')
        etree.SubElement(res_xml, 'version').text = "2010-11-15"
        etree.SubElement(res_xml, 'file-format').text = str(file_format)

        importer = etree.SubElement(res_xml, 'importer')
        etree.SubElement(importer, 'name').text = "ec2-upload-disk-image"
        etree.SubElement(importer, 'version').text = "1.0.0"
        etree.SubElement(importer, 'release').text = "2010-11-15"

        imports = etree.SubElement(res_xml, 'import')
        parts = etree.SubElement(imports, 'parts', count=str(len(res_list)))

        size = 0
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

        return "{0}\n{1}".format(
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            etree.tostring(res_xml, pretty_print=True))
