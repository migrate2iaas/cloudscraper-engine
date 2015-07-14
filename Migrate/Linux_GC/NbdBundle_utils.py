"""
NbdBundle_utils
~~~~~~~~~~~~~~~~~

This module provides NbdBundle_utils overrides for GCE utils
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import time
import sys
import os


sys.path.append('./submodules/compute-image-packages/gcimagebundle')
sys.path.append('./../submodules/compute-image-packages/gcimagebundle')
import gcimagebundlelib

from gcimagebundlelib.utils import *


class LoadNbdImage(object):
    """Mounts virtual disk via qemu-nbd"""
    # the port is random evert new run so nbd errors won't affect us 
    nbd_port = int(time.clock())%16
    
    def __init__(self, file_path):
        """ Inits object

         Args:
            file_path: a path to a file containing virtual disk image.      

          Returns:
            path to raw disk device to open
        """
        self._file_path = file_path
        # nbd creates 16 entries nbd0 thru nbd15
        self._nbd_path = "/dev/nbd" + str((LoadNbdImage.nbd_port%16))
        LoadNbdImage.nbd_port = (LoadNbdImage.nbd_port + 1) % 16

    def __enter__(self):
        """Map disk image as a device."""
    
        mountpath =  self._nbd_path 
        logging.debug("Starting qemu block device emulation " + mountpath + " from image " + self._file_path)
        modprobe_cmd = ['modprobe', 'nbd']
       
        output = RunCommand(modprobe_cmd)
        nbd_cmd = ["qemu-nbd", "-c" , mountpath, self._file_path]
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

        mountpath =  self._nbd_path
        nbd_cmd = ["qemu-nbd", "-d" , mountpath]
        output = RunCommand(nbd_cmd)

class Nbd_LoadDiskImage(object):
  """Loads raw disk image using kpartx."""

  nbd_port = int(time.clock())%16

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
    self._ndb_path = "/dev/nbd" + str((Nbd_LoadDiskImage.nbd_port%16))
    Nbd_LoadDiskImage.nbd_port = (Nbd_LoadDiskImage.nbd_port + 1) % 16



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
        try:
            kpartx_cmd = ['kpartx', '-d', '-v', '-s', self._file_path]
            RunCommand(kpartx_cmd)
        except Exception as e:
            logging.error("kpartx failed to release resources")
        
        if self._virtual_image:
            mountpath =  self._ndb_path
            nbd_cmd = ["qemu-nbd", "-d" , mountpath]
            output = RunCommand(nbd_cmd)
  def __enter__(self):
    """Map disk image as a device."""
    return 
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



class NbdOverride:
    """ Static class that keeps override functions for utils.py"""

    original_MakePartitionTable = None
    original_MakePartition = None
    original_GetPartitionStart = None
    original_RemovePartition = None
    original_GetDiskSize = None
    original_LoadDiskImage_class = None

    @staticmethod
    def init_override():
        """Singleton-like init"""
        if NbdOverride.original_MakePartitionTable == None:
            NbdOverride.original_MakePartitionTable = MakePartitionTable
            gcimagebundlelib.utils.MakePartitionTable = NbdOverride.ndb_MakePartitionTable
        if NbdOverride.original_MakePartition == None:
            NbdOverride.original_MakePartition = MakePartition
            gcimagebundlelib.utils.MakePartition = NbdOverride.ndb_MakePartition
        if NbdOverride.original_GetPartitionStart == None:
            NbdOverride.original_GetPartitionStart = GetPartitionStart
            gcimagebundlelib.utils.GetPartitionStart = NbdOverride.ndb_GetPartitionStart
        if NbdOverride.original_RemovePartition == None:
            NbdOverride.original_RemovePartition = RemovePartition
            gcimagebundlelib.utils.RemovePartition = NbdOverride.ndb_RemovePartition
        if NbdOverride.original_GetDiskSize == None:
            NbdOverride.original_GetDiskSize = GetDiskSize
            gcimagebundlelib.utils.GetDiskSize = NbdOverride.ndb_GetDiskSize
        if NbdOverride.original_LoadDiskImage_class == None:
            NbdOverride.original_LoadDiskImage_class = LoadDiskImage
            gcimagebundlelib.utils.LoadDiskImage = Nbd_LoadDiskImage
            

    @staticmethod
    def ndb_MakePartitionTable(file_path):
      """Create a partition table in a file.

      Args:
        file_path: A path to a file where a partition table will be created.
      """
      with LoadNbdImage(file_path) as path:
        NbdOverride.original_MakePartitionTable.im_func(path)

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
          NbdOverride.original_MakePartition.im_func(path, partition_type, fs_type, start, end)

    @staticmethod
    def ndb_RemovePartition(disk_path, partition_number):
      """removes partition"""
      with LoadNbdImage(file_path) as path:
          NbdOverride.original_RemovePartition.im_func(disk_path, partition_number)

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
          NbdOverride.original_GetPartitionStart.im_func(path ,partition_number)   

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
          NbdOverride.original_GetDiskSize.im_func(path)


#for initial debug
if __name__ == '__main__':
    NbdOverride.init_override()
    l1 = gcimagebundlelib.utils.LoadDiskImage("test")
    dic = l1.__dict__
    dr = dir(l1)
    with gcimagebundlelib.utils.LoadDiskImage("test") as l:
        dic = l.__dict__
        logging.info("done")