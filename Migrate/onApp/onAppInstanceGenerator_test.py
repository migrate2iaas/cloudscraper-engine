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
        
        #generator = onAppInstanceGenerator.onAppInstanceGenerator("activegrid-lax.xfernet.net" , "support@migrate2iaas.com", "8e4d09e0922cf6b7dff79a9c8d9b46f6e252bdc0" , 8 , minipad_image_id=30 , vmbuild_timeout = 60*180)
        generator = onAppInstanceGenerator.onAppInstanceGenerator("cloud.netcetera.co.uk" , "feoff@migrate2iaas.com", "866339f0fc021670ca6ac1803e5c1b73dbc3a21c" , 1 , preset_ip="146.247.49.62", minipad_vm_id="fhcja6edzrjcxa" , vmbuild_timeout = 60*180)

        instance = generator.makeInstanceFromImage("https://cloudscraper-test2.d3-lax.dincloud.com/WIN-9RJUUDQ3A9F-Cmanifest.xml?AWSAccessKeyId=91T0O18P61POALLD3ZKE&Expires=1429388566&Signature=Hzo%2F39GAKyL6Dv5YcimV%2FV0Wqis%3D" , None, "autotest-instance")
        volume = generator.makeVolumeFromImage("https://cloudscraper-test2.d3-lax.dincloud.com/WIN-9RJUUDQ3A9F-Cmanifest.xml?AWSAccessKeyId=91T0O18P61POALLD3ZKE&Expires=1429388566&Signature=Hzo%2F39GAKyL6Dv5YcimV%2FV0Wqis%3D" , None, "autotest-datadisk")

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