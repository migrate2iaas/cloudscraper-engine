"""
EHInstanceGenerator
~~~~~~~~~~~~~~~~~

This module provides EHInstanceGenerator class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import virtualmachine
import InstanceGenerator
import requests

import string
from random import sample, choice

import EHInstance

class EHInstanceGenerator(InstanceGenerator.InstanceGenerator):
    """generates EH instances (virtual servers)"""


    def __init__(self , apikey, apisecret, location):
        # We make it possible to use other ElasticStack clouds given by a direct link
        if location.find("https://") != -1 or location.find("http://") != -1:
            self.__hostname = location
        else:
            self.__hostname = 'https://api-'+location+'.elastichosts.com'

        self.__EH = requests.Session()
        self.__EH.auth = (apikey, apisecret)
        self.__EH.headers.update({'Content-Type': 'text/plain', 'Accept':'application/json'})

        return super(EHInstanceGenerator, self).__init__()

    def makeInstanceFromImage(self , imageid , initialconfig, instancename):
        """
        Starts server from the uploaded image, returns VmInstance
        Args:
            imageid - disk uuid
            initialconfig - ? 
            instancename - the instance name to create
        """
        chars = string.letters + string.digits
        length = 8
        createdata = "name " + instancename + "\n" + "cpu 1000"+"\n"+"persistent true"+"\n"+"password "+(''.join(sample(chars,length)))+"\nmem 1024"+\
            "\nide:0:0 disk"+"\nboot ide:0:0"+"\nide:0:0 "+imageid+"\nnic:0:model e1000"+"\nnic:0:dhcp auto"+"\nvnc auto"+"\nsmp auto";

        response = self.__EH.post(self.__hostname+"/servers/create/stopped" , data=createdata)
        if response.status_code != 200:
            logging.warning("!Unexpected status code returned by the ElasticHosts request: " + str(response) + " " + str(response.text))
            logging.warning("Headers: %s \n" , str(response.request.headers) )
            response.raise_for_status()
        instanceid = response.json()[u'server']
        logging.info(">>>>>>>>>>> New server " + instancename + "("+ instanceid +") created");
        return EHInstance.EHInstance(instanceid, self.__EH, self.__hostname)


    def makeVolumeFromImage(self , imageid , initialconfig, instancename):
        """ Creates a data volume. 
        For EH, really nothing
        """
        return imageid