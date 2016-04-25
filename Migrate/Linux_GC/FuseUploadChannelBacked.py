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

import DataExtent

class FuseUploadChannelBacked(LoggingMixIn, Operations):
    """ FUSE operation class callbacks """


    def __init__(self , cloud_options):
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
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        self.cache = defaultdict(bytes)

   
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
        else:
            #upload_channel = self.createChannel(path)
            #self.channels[path] = upload_channel
            upload_channel = None # we delay creation until first write

        self.fd += 1
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time() ,
                                upload_channel = upload_channel,
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
        return self.files[path]['upload_channel']


    def write(self, path, data, offset, fh):
        if self.files[path]['st_mode'] & S_IFDIR != 0:
            logging.info("Skipping write to dir")
            return len(data)
        #if self.files[path]['st_mode'] & S_IFLNK != 0:
        #    logging.info("Skipping write to link")
        #    return len(data)

        if self.data.has_key(path) == False:
            self.data[path] = defaultdict(bytes)
        if self.cached_data.has_key(path) == False:
            self.cached_data[path] = defaultdict(bytes)         
               
        # cache
        if self.cached_data[path].has_key(offset):
            self.cached_data[path][offset] = data
        else:
            self.data[path][offset] = data
            if len(self.data[path]) > 512:
                for key in sorted(self.data[path].keys()):
                    self.data[path].pop(key)
                    break
        #end cache

        if self.files[path]['st_size'] < offset + len(data):
            self.files[path]['st_size'] = offset + len(data)

        channel = self.getChannel(path)

        dataext = DataExtent.DataExtent(offset , len(data))
        dataext.setData(data)
        channel.uploadData(dataext)

        return len(data)

    def read(self, path, size, offset, fh):
        #logging.info(" FUSE READ " + str(size) + " Bytes OFFSET " + str(offset) )
        
        if self.cached_data.has_key(path):
            if self.cached_data[path].has_key(offset):
                data = self.cached_data[path][offset][:size]
                return data

        if self.data.has_key(path):
            if self.data[path].has_key(offset):
                data = self.data[path][offset][:size]
                if self.cached_data.has_key(path) == False:
                    self.cached_data[path] = defaultdict(bytes)
                self.cached_data[path][offset] = data
                logging.info(" FUSE READ " + str(size) + " Bytes OFFSET " + str(offset) + " added to cache")
                return data

        channel = self.getChannel(path)
        if channel.canDownload():
            logging.info("DOWNLOAD DATA BACK FROM CLOUD at OFFSET " + str(offset))
            data = channel.download(offset, size)
            if data:
                return data

        logging.info("GOT NO DATA TO SERVE at OFFSET " + str(offset))
        return str(bytearray(size))