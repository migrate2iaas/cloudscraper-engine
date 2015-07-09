"""
OpenStackInstanceGenerator
~~~~~~~~~~~~~~~~~

This module provides OpenStackInstanceGenerator class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------



import logging
import traceback
import novaclient
import keystoneclient.v2_0.client as ksclient
from keystoneclient.auth.identity import v2
from keystoneclient import session
from novaclient import client


import logging
import traceback
import time

import InstanceGenerator
import OpenStack

class OpenStackInstanceGenerator(InstanceGenerator.InstanceGenerator):
    """Generator of vm instances for OpenStack based clouds."""

    def __init__(self , server_url , tennant_name , username , password , version="2"):
        self.__server_url = server_url
        self.__tennant = tennant_name
        self.__username = username
        self.__password = password
        #keystone = ksclient.Client(auth_url=server_url,   username=username, password=password, tenant_name= tennant_name)
        #self.__auth = keystone.auth_token
        self.__nova = client.Client(version,username,password,tennant_name,server_url)
        self.__nova.authenticate()
        super(OpenStackInstanceGenerator, self).__init__()

    def makeInstanceFromImage(self , imageid, initialconfig, instancename):
        """generates cloud server instances from uploaded images"""
        images = self.__nova.images.list()
        flavors = self.__nova.flavors.list()
        flavor = flavors[0]
        servers =self.__nova.servers.list()
        
        image = self.__nova.images.get(imageid)
        self.__nova.servers.create(instancename , image , flavor=flavor)
        


    def makeVolumeFromImage(self , imageid , initialconfig, instancename):
        """generates cloud server instances from uploaded images"""
        raise NotImplementedError