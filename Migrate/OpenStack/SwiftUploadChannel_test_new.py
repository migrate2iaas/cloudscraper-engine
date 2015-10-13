

import sys

sys.path.append(sys.path[0]+'\.\..')
sys.path.append('.\..\OpenStack')
sys.path.append('.\OpenStack')



import unittest
import logging
import traceback
import sys

import os
import DataExtent
import time
#import OpenStackUploadChannel
from OpenStack import SwiftUploadChannel_new


class SwiftUploadChannel_test(unittest.TestCase):
    """SwiftUploadChannel class unittest"""

     #--------------------- Tests:
    
    # def test_small(self):
    #     """test1 desctiption"""
    #
    #     filename = 'D:\\cloudscraper-test-files\\small.file'
    #     size = os.stat(filename).st_size
    #
    #     channel = SwiftUploadChannel_new.SwiftUploadChannel_new(
    #         size,
    #         server_url="https://eu01-auth.webzilla.com:5000/v2.0",
    #         user_name="3186",
    #         tennant_name="2344",
    #         password="icafLFsmAOswwISn",
    #         container_name="testcontainer1",
    #         disk_name="small.file")
    #     channel.initStorage()
    #
    #     file = open(filename, "rb")
    #     datasize = channel.getTransferChunkSize()
    #     dataplace = 0
    #     while 1:
    #         try:
    #             data = file.read(datasize)
    #         except EOFError:
    #             break
    #         if len(data) == 0:
    #             break
    #         dataext = DataExtent.DataExtent(dataplace , len(data))
    #         dataplace = dataplace + len(data)
    #         dataext.setData(data)
    #         channel.uploadData(dataext)
    #
    #     channel.waitTillUploadComplete()
    #     channel.confirm()
    #     channel.close()
    #
    #     return


    def test_medium(self):
        """test1 desctiption"""

        filename = 'D:\\cloudscraper-test-files\\medium.file'
        size = os.stat(filename).st_size

        channel = SwiftUploadChannel_new.SwiftUploadChannel_new(
            size,
            server_url="https://eu01-auth.webzilla.com:5000/v2.0",
            username="3186",
            tennant_name="2344",
            password="icafLFsmAOswwISn",\
            disk_name="openstack-ubuntu.qcow2", container_name = "testcontainer2")
        channel.initStorage()

        file = open(filename, "rb")
        datasize = channel.getTransferChunkSize()
        dataplace = 0
        while 1:
            try:
                data = file.read(datasize)
            except EOFError:
                break
            if len(data) == 0:
                break
            dataext = DataExtent.DataExtent(dataplace, len(data))
            dataplace = dataplace + len(data)
            dataext.setData(data)
            channel.uploadData(dataext)

        channel.waitTillUploadComplete()
        channel.confirm()
        channel.close()

        return


    # def test_large(self):
    #     """test1 desctiption"""
    #
    #     filename = 'D:\\cloudscraper-test-files\\large_6gb.file'
    #     size = os.stat(filename).st_size
    #
    #     channel = SwiftUploadChannel_new.SwiftUploadChannel_new(
    #        size,
    #        server_url="https://eu01-auth.webzilla.com:5000/v2.0",
    #        user_name="3186",
    #        tennant_name="2344",
    #        password="icafLFsmAOswwISn",
    #        container_name="testcontainer1",
    #        disk_name="large_6gb.file")
    #     channel.initStorage()
    #
    #     file_object = open(filename, "rb")
    #     try:
    #         for piece in self.read_in_chunks(file_object, channel.getTransferChunkSize()):
    #             channel.uploadData(piece)
    #
    #         channel.waitTillUploadComplete()
    #         channel.confirm()
    #     except Exception as err:
    #         logging.error(err)
    #         channel.close()
    #         raise
    #
    #     file_object.close()
    #     channel.close()
    #     return

    #---------------------- Init and Deinit:
    def setUp(self):
        """sets up the object before any single test"""
        return

    def tearDown(self):
        """frees the resources after any single test"""
        return

    @classmethod
    def setUpClass(cls):
        """sets up the environment before its testing begun"""
        # use cls.__something if you wish to add some static objects into it
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.DEBUG)

    @classmethod
    def tearDownClass(cls):
        """frees any global resources allocated for the test """
        return


if __name__ == '__main__':
    unittest.main()