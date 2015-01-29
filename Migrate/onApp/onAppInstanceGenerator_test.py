"""
onAppInstanceGenerator_test
~~~~~~~~~~~~~~~~~

Unittest for CLASS class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------



import sys

sys.path.append('.\..')
sys.path.append('.\..\onApp')
sys.path.append('.\..\Windows')
sys.path.append('.\..\MiniPad')


sys.path.append('.\Windows')
sys.path.append('.\onApp')
sys.path.append('.\MiniPad')


import unittest
import logging
import traceback
import sys
import onAppInstanceGenerator
#import CLASS




class onAppInstanceGenerator_test(unittest.TestCase):
    """CLASS class unittest"""

    #--------------------- Tests:
    
    def test_create(self):
        """creates new VM"""
        
        generator = onAppInstanceGenerator.onAppInstanceGenerator("oadev.xfernet.net" , "feoff@migrate2iaas.com", "autotestCS@1" , 1 , preset_ip = "67.43.162.108" , minipad_vm_id = "b841dyafc8e6a2")

        instance = generator.makeInstanceFromImage("http://cloudscraper-1420772751-eu-west-1.s3.amazonaws.com/WIN-9RJUUDQ3A9F-Cmanifest.xml" , None, "autotest")

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