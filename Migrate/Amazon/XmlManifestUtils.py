
import urllib
import logging
import traceback




def getImageDataFromXmlData(xmldata):
    """returns tuple (volume-size-bytes,image-size-bytes,image-file-type)"""
    gb = 1024*1024*1024
    volume_size_bytes = 0
    image_file_size = 0
    imagetype = ''
    
    xmlheader = xmldata

    (head, sep ,tail) = xmlheader.partition("<file-format>")
    if tail:
        (head, sep ,tail) = tail.partition("</file-format>")
        imagetype = head

    (head, sep ,tail) = xmlheader.partition("<size>")
    if tail:
        (head, sep ,tail) = tail.partition("</size>")
        image_file_size = int(head , base = 10)
        logging.debug("The image of size " + str(image_file_size))
    else:
        logging.warning("!Couldn't parse the xml describing the import done " + xmlheader)
    (head, sep ,tail) = xmlheader.partition("<volume-size>")
    if tail:
        (head, sep ,tail) = tail.partition("</volume-size>")
        volume_size_bytes = int(head , base = 10) * gb
        logging.debug("The volume would be of size " + str(volume_size_bytes))
    else:
        logging.warning("!Couldn't parse the xml describing the import done" + xmlheader)
    (head, sep ,tail) = xmlheader.partition('parts count="')
    if tail:
        (head, sep ,tail) = tail.partition("\"")
        fragment_count = int(head)
        logging.debug("The volume would be of size " + str(fragment_count))
    else:
        logging.warning("!Couldn't parse the xml describing the import done" + xmlheader)

    
    return (volume_size_bytes, image_file_size , imagetype , fragment_count)

def getImageDataFromXml(xmlurl):
    """returns tuple (volume-size-bytes,image-size-bytes,image-file-type)"""
    gb = 1024*1024*1024
    volume_size_bytes = 0
    image_file_size = 0
    imagetype = ''
    
    urldata = urllib.urlopen(xmlurl) 
    return getImageDataFromXmlData(urldata.read())


class S3ManfiestBuilder:

    def __init__(self, tmpFileName, s3XmlKey, bucketname, s3connection, fileFormat='VHD'):
        self.__file = open(tmpFileName, "wb")
        self.__xmlKey = s3XmlKey
        self.__fileFormat = fileFormat
        self.__bucket = bucketname
        self.__S3 = s3connection

        return

    def buildHeader(self, bytesToUpload, resultingSizeGb, fragmentCount):
      #TODO: change file format
       header = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?> \n\t <manifest> \n\t\t <version>2010-11-15</version> \n\t\t <file-format>'+self.__fileFormat+'</file-format> \n\t\t <importer>'
       self.__file.write(header)

       #TODO: we emulate the standart utility XML (otherwise, the import won't work properly)
       importerver = '\n\t\t\t <name>ec2-upload-disk-image</name> \n\t\t\t <version>1.0.0</version> \n\t\t\t  <release>2010-11-15</release>'
       self.__file.write(importerver)
       
       linktimeexp_seconds = 60*60*24*15 # 15 days
       urldelete = self.__S3.generate_url( linktimeexp_seconds, method='DELETE', bucket=self.__bucket,  key=self.__xmlKey, force_http=False)
       selfdestruct = '\n\t\t </importer> \n\t\t  <self-destruct-url>'+urldelete.replace('&' ,'&amp;')+'</self-destruct-url> '
       self.__file.write(selfdestruct)

       importvol = '\n\t\t <import> \n\t\t\t <size>'+ str(bytesToUpload) + '</size> \n\t\t\t <volume-size>' + str(resultingSizeGb) + '</volume-size>' + '\n\t\t\t <parts count="'+str(fragmentCount)+'">'
       self.__file.write(importvol)
         
       return

    def addUploadedPart(self , index , rangeStart , rangeEnd , keyName):
        
        linktimeexp_seconds = 60*60*24*5   # 5 days

        indexstr = '\n\t\t\t <part index="'+str(index)+'">\n\t\t\t\t '+'<byte-range end="' + str (rangeEnd) + '" start="'+ str(rangeStart) +'" />'
        self.__file.write(indexstr)

        keystr = '\n\t\t\t\t <key>'+str(keyName)+'</key>'
        self.__file.write(keystr)
          
        urlhead = self.__S3.generate_url( linktimeexp_seconds, method='HEAD', bucket=self.__bucket, key=keyName, force_http=False)
        gethead = '\n\t\t\t\t <head-url>'+urlhead.replace('&' ,'&amp;')+'</head-url>'
        self.__file.write(gethead)

        urlget = self.__S3.generate_url( linktimeexp_seconds, method='GET', bucket=self.__bucket, key=keyName, force_http=False)
        getstr = '\n\t\t\t\t <get-url>'+urlget.replace('&' ,'&amp;')+'</get-url>'
        self.__file.write(getstr)

        urldelete = self.__S3.generate_url( linktimeexp_seconds, method='DELETE', bucket=self.__bucket, key=keyName, force_http=False)
        getdelete = '\n\t\t\t\t <delete-url>'+urldelete.replace('&' ,'&amp;')+'</delete-url>'
        self.__file.write(getdelete)

        partend = '\n\t\t\t </part>'
        self.__file.write(partend)

        return

    def finalize(self):
        end = '\n </parts>\n </import> \n </manifest>\n'
        self.__file.write(end)
        self.__file.close()
        return 