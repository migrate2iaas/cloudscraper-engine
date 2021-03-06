﻿

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
import threading
import UploadManifest
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

        #
        # webzilla usage
        #
        manifest = UploadManifest.ImageManifestDatabase(
            UploadManifest.ImageDictionaryManifest, "C:\\backup-manifest", None, threading.Lock(),
            increment_depth=1, db_write_cache_size=20,
            use_dr=False, resume=False, volname="B", target_id="HOME-PC")

        channel = SwiftUploadChannel_new.SwiftUploadChannel_new(
            size,
            server_url="https://eu01-auth.webzilla.com:5000/v2.0",
            username="3186",
            tennant_name="2344",
            password="icafLFsmAOswwISn",\
            disk_name="medium.file",
            container_name="testcontainer2",
            manifest_path="D:\\backup-manifest",
            chunksize=1024*1024,
            increment_depth=3,
            swift_use_slo=False,
            manifest=manifest)

        #
        # cloudex.rs usage
        #
        # channel = SwiftUploadChannel_new.SwiftUploadChannel_new(
        #     size,
        #     server_url="http://87.237.203.66:5000/v2.0",
        #     username="migrate2iaas",
        #     tennant_name="migrate2iaas",
        #     password="migrate2iaas",
        #     disk_name="test/medium.file",
        #     container_name="cloudscraper-pub4",
        #     manifest_path="D:\\backup-manifest",
        #     chunksize=10*1024*1024,
        #     increment_depth=3,
        #     swift_use_slo=False,
        #     use_dr=True
        #     )
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
            channel.getOverallDataSkipped()
            channel.getOverallDataTransfered()

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