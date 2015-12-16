

import sys

sys.path.append(sys.path[0]+'.\\..')
sys.path.append('./..')
sys.path.append('./../onApp')
sys.path.append('./../Windows')
sys.path.append('./../MiniPad')
sys.path.append('./../Amazon')

sys.path.append('./Windows')
sys.path.append('./onApp')
sys.path.append('./Amazon')
sys.path.append('./MiniPad')


import unittest
import logging
import traceback
import EC2MinipadInstanceGenerator
#import CLASS




class EC2MinipadInstanceGenerator_test(unittest.TestCase):
    """CLASS class unittest"""

    #--------------------- Tests:
    
    def test_create(self):
        self.__key = 'AKIAIY2X62QVIHOPEFEQ'
        self.__secret = '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'
        generator = EC2MinipadInstanceGenerator.EC2MinipadInstanceGenerator(region = "ap-southeast-2" , ami = "ami-824c17e1" , \
            key = self.__key , secret = self.__secret, zone = "ap-southeast-2a", instance_type = "t2.small", subnet = "subnet-26fe7551" , security_group="sg-9a586aff", volume_type = 'gp2')

        instance = generator.makeInstanceFromImage("http://twftemp.s3.amazonaws.com/sbsnewTWFSBS-Cmanifest.xml" , None, "test-instance")
        #volume = generator.makeVolumeFromImage("http://twftemp.s3.amazonaws.com/sbsnewTWFSBS-Cmanifest.xml" , None, "autotest-datadisk")
        instance.finalize()

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