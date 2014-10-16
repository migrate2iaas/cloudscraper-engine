"""
The Linux python module declaration
~~~~~~~~~~~~~~~~~

This module provides Linux singleon class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import sys

sys.path.append('./../compute-image-packages/gcimagebundle')
sys.path.append('./../../compute-image-packages/gcimagebundle')
from gcimagebundlelib import *
from gcimagebundlelib import utils


import filecmp
import unittest
import shutil
import logging
import os
import tempfile

import LinuxAdjustOptions
import LinuxBackupSource
import LinuxSystemInfo
import LinuxVolumeInfo
import LinuxBlockDevice
import LinuxBackupAdjust

import TransferTarget


import re
from subprocess import *


def RenameFile(src_files , dest):
    """a small hack function"""
    for file in src_files:
        os.rename(file , dest)
        #touch() -like command. just create an empty file so google code could delete it afterwards
        with open(file, 'a'):
            os.utime(file, None)
        dest = dest + "1"

# NOTE: we redefine module function to change the behaviour a bit...
# a tricky hack
utils.TarAndGzipFile = RenameFile


class BundleTransferTarget(TransferTarget.TransferTarget):
    """Target to pass file data to the target"""

    def __init__ (self , bundle_object, media , linux , guest_platform , include_mounts = False):
        self.__bundle = bundle_object
        self.__media = media
        self.__linux = linux
        self.__guest_platform = guest_platform
        self.__include_mounts = include_mounts


    def transferFile(self , fileToBackup):
        """
            Transfers directory and all its contains
            NOTE: it's workaround. It transfers all root given
        """
        #TODO: kinda interface transfering data on file lists not files only
        # the adapter could just accumulate number of file then start syncing...
        device = self.__linux.findDeviceForPath(str(fileToBackup))
        if not device:
            logging.error("!!!ERROR: Cannot find corresponding device for "  + str(fileToBackup) );

        self.__bundle.AddDisk(device)
        self.__bundle.AddSource(str(fileToBackup))
        
        #Setting the root dir as only file to backup
        rootdir = str(fileToBackup)

        # Merge platform specific exclude list, mounts points
        # and user specified excludes
        excludes = guest_platform.GetExcludeList()
        # don't support user exclude list for now
        #if options.excludes:
        #    excludes.extend([exclude_spec.ExcludeSpec(x) for x in
        #                    options.excludes.split(',')])
        logging.info('exclude list: %s', ' '.join([x.GetSpec() for x in excludes]))
        self.__bundle.AppendExcludes(excludes)
        if not self.__include_mounts:
            mount_points = utils.GetMounts(rootdir)
        logging.info('ignoring mounts %s', ' '.join(mount_points))
        self.__bundle.AppendExcludes([exclude_spec.ExcludeSpec(x, preserve_dir=True) for x
                                in utils.GetMounts(rootdir)])
        self.__bundle.SetPlatform(self.__guest_platform)

        #Do bundling
        self.__bundle.Verify()
        (fs_size, digest) = self.__bundle.Bundleup()


    # transfers file data only, no metadata should be written
    def transferFileData(self, fileName, fileExtentsData):
        """
            Transfers data only
        """
        raise NotImplementedError

    # transfers file data only
    def transferRawData(self, volExtents):
        raise NotImplementedError

    # transfers raw metadata, it should be precached
    def transferRawMetadata(self, volExtents):
        raise NotImplementedError

    #deletes file transfered
    def deleteFileTransfer(self , filename):
        raise NotImplementedError

    #cancels the transfer and deletes the transfer target
    def cancelTransfer(self):
        raise NotImplementedError

    def getMedia(self):
        return self.__media
        

class AttrDict(dict):
    """helper class for attr dictionary to make things comaptible with config code """
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

class Linux(object):
    """
    That's the root Linux class incapsulating GCE image bundle logic
    The primary challange of creating a good adapter is the following issue:
    The block_disk.RootFsRaw object is both source and target of the transfer
    For now it's very dependent on the Migrator workflow
    """


    def createBundleTransferTarget(self , media, size):
        """a specific GCE method"""

        #TODO: make assert media size equals file size
        options = AttrDict()
        options['fs_size']  = size
        options['skip_disk_space_check'] = True;
        #TODO: Should pass somehow
        options['file_system'] = "ext3"

        
        try:
            guest_platform = platform_factory.PlatformFactory(
                options.root_directory).GetPlatform()
        except platform_factory.UnknownPlatformException:
            logging.critical('Platform is not supported.'
                             ' Platform rules can be added to platform_factory.py.')
            raise RuntimeError("Linux distro is not supported");

        # if options.file_system is not set - sets it to prefered guest
        file_system = GetTargetFilesystem(options, guest_platform)

        #it's the transfer target
        bundle_object = block_disk.RootFsRaw(options.fs_size, file_system, options.skip_disk_space_check)

        scratch_dir = None
        try:
            scratch_dir = media.getFilePath()
            if os.path.isdir(scratch_dir) == False:
                scratch_dir = os.path.pardir(scratch_dir)
        except NotImplementedError as e:
            scratch_dir = None

        if not scratch_dir:
            scratch_dir = tempfile.mkdtemp()
        # TODO: should tie up with dir
        bundle_object.SetScratchDirectory(scratch_dir)

        return BundleTransferTarget(bundle_object , media , self)

    
    def createSystemAdjustOptions(self):

        not_full_disk = self.getSystemDriveName()[-1].isdigit()
        if not_full_disk:
            logging.info("Create a disk based on single partition")
         # we should get specific configs here to generate the correct config
        options = LinuxAdjustOptions.LinuxAdjustOptions(is_full_disk = (not_full_disk == False)) 
               

        return options

    def getSystemDataBackupSource(self):
        logging.debug("Getting the system backup source") 
        
        #get system disk
        systemdisk = self.getSystemDriveName()

        lindisk = LinuxBlockDevice.LinuxBlockDevice(systemdisk)

        return lindisk
    
    def getDataBackupSource(self , volume):
        logging.debug("Getting data backup source") 
        
        #get system disk
        systemdisk = volume

        lindisk = LinuxBlockDevice.LinuxBlockDevice(systemdisk)

        return lindisk

    def getSystemInfo(self):
        return LinuxSystemInfo.LinuxSystemInfo()

    def findDeviceForPath(self , path):
        p1 = Popen(["df" , path], stdout=PIPE)
        output = p1.communicate()[0]
        lastline = output.split("\n")[1]
        voldev = lastline[:lastline.find(" ")]
        return voldev

    def __findLvmDev(self , volgroup):
        p1 = Popen(["lvdisplay" , "-m", volgroup], stdout=PIPE)
        output = p1.communicate()[0]
        
        if str(output).count("Physical volume") > 1:
            logging.error("!!!ERROR: LVM config is too complex to parse!")
            raise LookupError()

        match = re.search( "Physical volume([^\n]*)", output )
        if match == None:
            logging.error("!!!ERROR: Couldn't parse LVM config! ")
            logging.error("Config " + output)
            raise LookupError()

        volume = match.group(1)
        return volume.strip()

    def __findMountPoint(self , path):
        p1 = Popen(["df" , path], stdout=PIPE)
        output = p1.communicate()[0]
        lastline = output.split("\n")[1]
        mnt = lastline[lastline.rfind(" "):]
        return str(mnt).strip()

    def getSystemDriveName(self):
        rootdev = self.findDeviceForPath("/")
        bootdev = self.findDeviceForPath("/boot")

        
        logging.info("The root device is " + rootdev);
        logging.info("The boot device is " + bootdev);

        # try to see where it resides. it's possible to be an lvm drive
        if rootdev.count("mapper/VolGroup-") > 0: 
             volgroup = str(rootdev).replace('mapper/VolGroup-', 'VolGroup/')
             rootdev = self.__findLvmDev(volgroup)
             logging.info("LVM " + volgroup + " resides on " + rootdev);

        #substract the last number making /dev/sda from /dev/sda1. 
        rootdrive = rootdev[:-1]
        bootdrive = bootdev[:-1]


        if rootdrive != bootdrive:
            logging.warn("!Root and boot drives are on different physical disks. The configuration could lead to boot failures.")
        
        try:
            # In the current impl we do full disk backup
            if os.stat(rootdrive):
                return rootdrive
        except Exception as e:
            #supper-floppy like disk then
            logging.info("There is no " + rootdrive + " device, treating " +  rootdev+ " as system disk")
            return rootdev

     