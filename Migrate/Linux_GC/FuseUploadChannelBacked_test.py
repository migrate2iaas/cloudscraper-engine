"""
Unittest for fuse
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2016 Migrate2Iaas"
#---------------------------------------------------------



import sys

sys.path.append('.\..')
sys.path.append('.\..\Linux_GC')
sys.path.append('.\..\Amazon')

sys.path.append('.\..')
sys.path.append('.\Linux_GC')
sys.path.append('.\Amazon')


import unittest
import logging
import traceback
import sys
import FuseUploadChannelBacked


class DummyChannel(object):
    
    def __init__(self , parm):
        self._p = parm
        pass

    def initStorage(self):
        return ""

    def download(self, offset , size = None):
        return ""

class DummyChannelGenerator(object):

    def __init__(self , parm):
        self._p = parm
        pass

    def generateUploadChannel(self , size, targetid):
        return DummyChannel(1)



class FuseUploadChannelBacked_test(unittest.TestCase):
    """CLASS class unittest"""

    #--------------------- Tests:
    
    

    def test_create(self):
        """create class object"""

        channel = DummyChannelGenerator(1)
        fuse = FuseUploadChannelBacked.FuseUploadChannelBacked(cloud_options=channel)
        fuse.create("filename" , 0)
        data = "233213"
        fuse.write("filename", data, 5, 0)
        newdata = fuse.read("filename" , len(data) , 5 , 0)
        self.assertEqual(data , newdata)

        return

    
    def test_testname2(self):
        """test2 desctiption"""
        return


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