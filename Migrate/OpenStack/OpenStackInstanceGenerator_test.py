"""
onAppInstanceGenerator_test
~~~~~~~~~~~~~~~~~

Unittest for CLASS class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------



import sys

sys.path.append(sys.path[0]+'\.\..')
sys.path.append('.\..\OpenStack')
sys.path.append('.\OpenStack')



import unittest
import logging
import traceback
import sys
import OpenStackInstanceGenerator
#import CLASS




class onAppInstanceGenerator_test(unittest.TestCase):
    """CLASS class unittest"""

    #--------------------- Tests:
    
    def test_create(self):
        """creates new VM"""
        self.generator.makeInstanceFromImage("d52d5e69-f000-43be-be77-fc686b8823ee" , None, "cloduscraper-test-server")
        
        return



    #---------------------- Init and Deinit:
    def setUp(self):
        """sets up the object before any single test"""
        #self.generator = OpenStackInstanceGenerator.OpenStackInstanceGenerator(server_url="https://eu01-auth.webzilla.com:5000/v2.0" , username="3186" , tennant_name="2344" , password = "icafLFsmAOswwISn" )
        self.generator = OpenStackInstanceGenerator.OpenStackInstanceGenerator(server_url="https://auth.nl01.cloud.webzilla.com:5000/v2.0" , username="3186" , tennant_name="2344" , password = "QpLQCTrJjeoWNJaf" )
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