"""

A class that implements FUSE filesystem where every file is stored inside of UploadChannel


"""

from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


from cachetools import LRUCache

import DataExtent


class BlockCache(LRUCache):

    def __init__(self, maxsize, channel, getsizeof=None):
        missing = self.missing
        super(BlockCache,self).__init__(maxsize, missing, getsizeof)
        self.__channel = channel

    def missing(self , offset):
        logging.info("DOWNLOAD DATA BACK FROM CLOUD at OFFSET " + str(offset))
        data = self.__channel.download(offset , size = None)
        logging.info("Download complete at offset " + str(offset) + " size = " + str(len(data)))
        return data
        
    def popitem(self):
        (offset, data) = super(BlockCache,self).popitem()
        dataext = DataExtent.DataExtent(offset , len(data))
        dataext.setData(data)
        logging.info("UPLOAD DATA TO CLOUD at OFFSET" + str(offset) + " size " + str(len(data)))
        self.__channel.uploadData(dataext)

    def __repr__(self):
        return '%s(maxsize=%d, currsize=%d)' % (
            self.__class__.__name__,
            self.__maxsize,
            self.__currsize,
        )


class FuseUploadChannelBacked(LoggingMixIn, Operations):
    """ FUSE operation class callbacks """


    def __init__(self , cloud_options , chunks_to_cache = 128):
        """
        Params:
            cloud_options: CloudConfig.CloudConfig - cloud config that can create upload channels via generateUploadChannel() method
        """
        self.cloud_options = cloud_options
        self.files = {}
        self.channels = {}
        self.data = {} #defaultdict(bytes)
        self.cached_data = {}
        self.fd = 0
        now = time()
        self.chunks_to_cache = chunks_to_cache
       
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        # cache stored on the local disk
        # self.cache_image_factory = cache_image_factory
        # self.cache_path = cache_path
        

   
    #STANDARD FROM EXAMPLE
    def chmod(self, path, mode):
        self.files[path]['st_mode'] &= 0o770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

   

    def getattr(self, path, fh=None):
        if path not in self.files:
            raise FuseOSError(ENOENT)

        return self.files[path]

    def getxattr(self, path, name, position=0):
        attrs = self.files[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

        self.files['/']['st_nlink'] += 1

    
    def readdir(self, path, fh):
        return ['.', '..'] + [x[1:] for x in self.files if x != '/']

    def readlink(self, path):
        return self.data[path][0]

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        chunk = 4096
        overall_size = 2048#2048#1024*1024*1024
        return dict(f_bsize=chunk
                    , f_blocks=overall_size, f_bavail=overall_size)

    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))

        self.data[target] = source

    def unlink(self, path):
        self.files.pop(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    ### MODIFIED:

    def createChannel(self , path , size):
        """method to generate channels by path"""
        channel = self.cloud_options.generateUploadChannel(size, targetid=path)
        channel.initStorage()
        return channel

    def create(self, path, mode):
        # TODO: here we seek for existing UploadChannel or create a new one
        if self.channels.has_key(path):
            upload_channel = self.channels[path]  
            cache = self.caches[path]  
        else:
            #upload_channel = self.createChannel(path)
            #self.channels[path] = upload_channel
            upload_channel = None # we delay creation until first write
            cache = None
        

        self.fd += 1
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time() ,
                                upload_channel = upload_channel,
                                cache = cache,
                                fds = [self.fd] );
        
        return self.fd

    def open(self, path, flags):
        self.fd += 1
        self.files[path]["fds"].append(self.fd)
        return self.fd

    def truncate(self, path, length, fh=None):
        self.files[path]['st_size'] = length

    def getChannel(self, path):
        size = self.files[path]['st_size']
        if self.files[path]['upload_channel'] == None:
            self.files[path]['upload_channel'] = self.createChannel(path, size)
            self.files[path]['caches'] = BlockCache(self.chunks_to_cache, self.files[path]['upload_channel'])

         #   self.files[path]['cache'] = cache_image_factory.createMedia(path, size)
        return self.files[path]['upload_channel']


    def write(self, path, data, offset, fh):
        if self.files[path]['st_mode'] & S_IFDIR != 0:
            logging.info("Skipping write to dir")
            return len(data)

        channel = self.getChannel(path)
        
        cacheline = channel.getTransferChunkSize()
        cacheline_start = int(offset/cacheline)*cacheline
        cacheline_offset = offset - cacheline_start

        cached = self.files[path]['caches'][cacheline_start]
        if not cached:
            cached = str(bytearray(cacheline))
        
        cached = cached[0:cacheline_offset] + data + cached[cacheline_offset + len(data):] 
        self.files[path]['caches'][cacheline_start] = cached
        
        return len(data)

    def read(self, path, size, offset, fh):
        if self.files[path]['st_mode'] & S_IFDIR != 0:
            logging.info("Skipping read dir")
            return ""

        channel = self.getChannel(path)
        
        cacheline = channel.getTransferChunkSize()
        cacheline_start = int(offset/cacheline)*cacheline
        cacheline_offset = offset - cacheline_start

        cached = self.files[path]['caches'][cacheline_start]

        if not cached:
            return str(bytearray(size))

        if cacheline_offset + size < cacheline:   
            return cached[cacheline_offset:cacheline_offset + size]
        
        #TODO: merge several cacheline if needed
        logging.warn("!Reading one cacheline while more data is requested")
        return cached[cacheline_offset:]