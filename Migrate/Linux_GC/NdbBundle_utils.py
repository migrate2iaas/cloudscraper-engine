"""
NdbBundle_utils
~~~~~~~~~~~~~~~~~

This module provides NdbBundle_utils overrides for GCE utils
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import time

from gcimagebundlelib.utils import *



class LoadNbdImage(object):
    """Mounts virtual disk via qemu-nbd"""
    def __init__(self, file_path):
        """ Inits object

         Args:
            file_path: a path to a file containing virtual disk image.      

          Returns:
            path to raw disk device to open
        """
        self._file_path = file_path
        self._nbd_path = "/dev/nbd" + str(hex(int(time.time())))

    def __enter__(self):
        """Map disk image as a device."""
    
        mountpath =  self._nbd_path 
        logging.debug("Starting qemu block device emulation " + mountpath + " from image " + self._file_path)
        modprobe_cmd = ['modprobe', 'nbd']
       
        output = RunCommand(modprobe_cmd)
        nbd_cmd = ["qemu-nbd", "-c" , mountpath, file_path]
        output = RunCommand(nbd_cmd)
        return mountpath

    def __exit__(self, unused_exc_type, unused_exc_value, unused_exc_tb):
        """Unmap disk image as a device.

        Args:
          unused_exc_type: unused.
          unused_exc_value: unused.
          unused_exc_tb: unused.
        """
        SyncFileSystem()
        time.sleep(2)

        mountpath =  self._ndb_path
        nbd_cmd = ["qemu-nbd", "-d" , mountpath]
        output = RunCommand(nbd_cmd)

class LoadDiskImage(object):
  """Loads raw disk image using kpartx."""

  def __init__(self, file_path, virtual_image = True):
    """Initializes LoadDiskImage object.

    Args:
      file_path: a path to a file containing raw disk image.
      
      virtual_image: a boolean specifying whether the file is virtual image (not raw file) supported by qemu-img

    Returns:
      A list of devices for every partition found in an image.
    """
    self._file_path = file_path
    self._virtual_image = virtual_image
    self._ndb_path = "/dev/nbd0"

  def __enter__(self):
    """Map disk image as a device."""
    SyncFileSystem()

    mountpath = self._file_path

    # VFedorov: we shall use non-kpartx implimentation when image is not raw
    if self._virtual_image:
        mountpath =  self._ndb_path
        modprobe_cmd = ['modprobe', 'nbd']
        logging.info(">>> Starting qemu block device emulation")
        output = RunCommand(modprobe_cmd)
        nbd_cmd = ["qemu-nbd", "-c" , mountpath, file_path]
        output = RunCommand(nbd_cmd)

    kpartx_cmd = ['kpartx', '-a', '-v', '-s', mountpath]
    output = RunCommand(kpartx_cmd)
    devs = []
    for line in output.splitlines():
      split_line = line.split()
      if (len(split_line) > 2 and split_line[0] == 'add'
          and split_line[1] == 'map'):
        devs.append('/dev/mapper/' + split_line[2])
    time.sleep(2)
    return devs


    def __exit__(self, unused_exc_type, unused_exc_value, unused_exc_tb):
        """Unmap disk image as a device.

        Args:
          unused_exc_type: unused.
          unused_exc_value: unused.
          unused_exc_tb: unused.
        """
        SyncFileSystem()
        time.sleep(2)

        #TODO: should check if kpartx was done previously
        #may fail on the faulty path
        kpartx_cmd = ['kpartx', '-d', '-v', '-s', self._file_path]
        RunCommand(kpartx_cmd)

        if self._virtual_image:
            mountpath =  self._ndb_path
            nbd_cmd = ["qemu-nbd", "-d" , mountpath]
            output = RunCommand(nbd_cmd)


class NdbOverride:
    """ Static class that keeps override functions for utils.py"""

    original_MakePartitionTable = None
    original_MakePartition = None
    original_GetPartitionStart = None
    original_RemovePartition = None
    original_GetDiskSize = None
    original_InstallGrub = None

    @staticmethod
    def init_override():
        """Singleton-like init"""
        if NdbOverride.original_MakePartitionTable == None:
            NdbOverride.original_MakePartitionTable = MakePartitionTable
            gcimagebundlelib.utils.MakePartitionTable = NdbOverride.ndb_MakePartitionTable
        if NdbOverride.original_MakePartition == None:
            NdbOverride.original_MakePartition = MakePartition
            gcimagebundlelib.utils.MakePartition = NdbOverride.ndb_MakePartition
        if NdbOverride.original_GetPartitionStart == None:
            NdbOverride.original_GetPartitionStart = GetPartitionStart
            gcimagebundlelib.utils.GetPartitionStart = NdbOverride.ndb_GetPartitionStart
        if NdbOverride.original_RemovePartition == None:
            NdbOverride.original_RemovePartition = RemovePartition
            gcimagebundlelib.utils.RemovePartition = NdbOverride.ndb_RemovePartition
        if NdbOverride.original_GetDiskSize == None:
            NdbOverride.original_GetDiskSize = GetDiskSize
            gcimagebundlelib.utils.GetDiskSize = NdbOverride.ndb_GetDiskSize
        if NdbOverride.original_InstallGrub == None:
            NdbOverride.original_InstallGrub = InstallGrub
            gcimagebundlelib.utils.InstallGrub = NdbOverride.ndb_InstallGrub

    @staticmethod
    def ndb_MakePartitionTable(file_path):
      """Create a partition table in a file.

      Args:
        file_path: A path to a file where a partition table will be created.
      """
      with LoadNbdImage(file_path) as path:
        original_MakePartitionTable(path)

    @staticmethod
    def ndb_MakePartition(file_path, partition_type, fs_type, start, end):
      """Create a partition in a file.

      Args:
        file_path: A path to a file where a partition will be created.
        partition_type: A type of a partition to be created. Tested option is msdos.
        fs_type: A type of a file system to be created. For example, ext2, ext3,
          etc.
        start: Start offset of a partition in bytes.
        end: End offset of a partition in bytes.
      """
      with LoadNbdImage(file_path) as path:
          original_MakePartition(path, partition_type, fs_type, start, end)

    @staticmethod
    def ndb_RemovePartition(disk_path, partition_number):
      """removes partition"""
      with LoadNbdImage(file_path) as path:
          original_RemovePartition(disk_path, partition_number)

    @staticmethod
    def ndb_GetPartitionStart(disk_path, partition_number):
      """Returns the starting position in bytes of the partition.

      Args:
        disk_path: The path to disk device.
        partition_number: The partition number to lookup. 1 based.

      Returns:
        The starting position of the first partition in bytes.

      Raises:
        subprocess.CalledProcessError: If running parted fails.
        IndexError: If there is no partition at the given number.
      """
      with LoadNbdImage(disk_path) as path:
          original_GetPartitionStart(path ,partition_number )   

    @staticmethod
    def ndb_GetDiskSize(disk_file):
      """Returns the size of the disk device in bytes.

      Args:
        disk_file: The full path to the disk device.

      Returns:
        The size of the disk device in bytes.

      Raises:
        subprocess.CalledProcessError: If fdisk command fails for the disk file.
      """
      with LoadNbdImage(disk_path) as path:
          original_GetDiskSize(path)


    @staticmethod
    def ndb_InstallGrub(boot_directory_path , disk_file_path):
      """Adds Grub boot loader to the disk and points it to boot from the partition"""
      with LoadNbdImage(disk_file_path) as path:
           original_GetPartitionStart(boot_directory_path ,path)