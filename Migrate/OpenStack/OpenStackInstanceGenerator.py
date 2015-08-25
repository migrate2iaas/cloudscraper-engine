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
from cinderclient.v2 import client as ciclient

import logging
import traceback
import time

import InstanceGenerator
import OpenStack

import VmInstance
import VmVolume


class OpenStackInstance(VmInstance.VmInstance):
    """OpenStack instance"""

    def __init__(self, server):
        self.__server = server

    def run(self):
        """starts instance"""
        self.__server.start()

    def stop(self):
        """stops instance"""
        self.__server.stop()

    def attachDataVolume(self):
        """attach data volume"""
        raise NotImplementedError

    def getIp(self):
        """returns public ip string"""
        return self.__server['accessIPv4']

    def deallocate(self , subresources=True):
        """deallocates a VM
            Args:
            subresources: Boolean - if True, deallocates all associated resources (disks, ips). Deallocates only the vm itself otherwise
        """
        self.__server.delete()

class OpenStackNovaVolume(VmVolume.VmVolume):
    """represents a volume to be created when the server created - on the attachement phase"""

    device_letter = "p"

    def __init__(self , nova , volume):
        self.__nova = nova
        self.__volume = volume
        return 

    def getId(self):
        """returns cloud id of the volume"""
        return self.__image.id

    def attach(self, vm_instance_id):
        """
        Attaches to the machine specified by instance_id
        """
        self.__nova.volumes.create_server_volume(vm_instance_id , self.__volume.id , "/dev/xvd"+OpenStackVolume.device_letter)
        # move from xvdp to xvdf
        OpenStackVolume.device_letter = chr(ord(OpenStackVolume.device_letter) - 1)

    def __str__(self):
        return str(self.getId())

class OpenStackrVolume(VmVolume.VmVolume):
    """represents a volume in OpenStack"""

    device_letter = "p"

    def __init__(self , volume):
        self.__volume = volume
        return 

    def getId(self):
        """returns cloud id of the volume"""
        return self.__volume.id

    def attach(self, vm_instance_id):
        """
        Attaches to the machine specified by instance_id
        """
        self.__volume.attach(vm_instance_id , "/dev/xvd"+OpenStackVolume.device_letter)
        # move from xvdp to xvdf
        OpenStackVolume.device_letter = chr(ord(OpenStackVolume.device_letter) - 1)

    def __str__(self):
        return str(self.getId())


class OpenStackInstanceGenerator(InstanceGenerator.InstanceGenerator):
    """Generator of vm instances for OpenStack based clouds."""

    def __init__(self , server_url , tennant_name , username , password , vmbuild_timeout_sec = 1200 , version="2"):
        self.__server_url = server_url
        self.__tennant = tennant_name
        self.__username = username
        self.__password = password
        #keystone = ksclient.Client(auth_url=server_url,   username=username, password=password, tenant_name= tennant_name)
        #self.__auth = keystone.auth_token
        self.__nova = client.Client(version,username,password,tennant_name,server_url)
        self.__nova.authenticate()

        try:
            self.__cinder = ciclient.Client(username=username,api_key=password,project_id=tennant_name,auth_url=server_url,service_type='volume')
            self.__cinder.authenticate()
        except Exception as e:
            logging.warning("!Cinder block storage is unavailable");
            logging.warning(traceback.format_exc())

        self.__vmbuild_timeout_sec = int(vmbuild_timeout_sec)
        super(OpenStackInstanceGenerator, self).__init__()

    def makeInstanceFromImage(self , imageid, initialconfig, instancename):
        """generates cloud server instances from uploaded images"""

        images = self.__nova.images.list()
        logging.info("Images found:")
        for image in images:
            logging.info(str(image.__dict__))
        flavors = self.__nova.flavors.list()
        flavor = flavors[0]
        servers =self.__nova.servers.list()
        for server in servers:
            logging.info(str(server.__dict__))

        nics = None
        #TODO: to the config
        networkid = '1a29f2c3-27f5-4473-9bcc-483fab79e10e'
        if initialconfig:
           if initialconfig.getSubnet():
               networkid = initialconfig.getSubnet()

        networks =self.__nova.networks.list()
        for network in networks:
            logging.info(str(network.__dict__))
            if str(network.id) == str(networkid):
                nic = dict()
                nic['net-id'] = network.id
                nics = list()
                nics.append(nic)
        
        
        image = self.__nova.images.get(imageid)
        logging.info(">>> Creating new server from image " + imageid)
        #TODO: get metadata from configs
        metadata = {'isolate_os':'windows' , 'requires_ssh_key':'false' , 'windows12':'true'}
        self.__nova.images.set_meta(image, metadata)

        server = self.__nova.servers.create(instancename , image , flavor=flavor, nics = nics)

        #wait for server to be created
        waited = 0
        interval_to_wait = self.__vmbuild_timeout_sec / 30 + 1
        while True:
            time.sleep(interval_to_wait)
            waited = waited + interval_to_wait
            if (waited > interval_to_wait):
                logging.warning("! Server is still building, aboritng wait operation.")
            servers = self.__nova.servers.list()
            for upd_server in servers:
                 if server.id == upd_server.id:
                     server = upd_server
            if not server.__dict__['OS-EXT-STS:vm_state'] == "building":
                # sometimes this parm is not set just when build is requested
                break
        
        logging.info("Server in state " + server.__dict__['OS-EXT-STS:vm_state'])
        if (server.__dict__['OS-EXT-STS:vm_state'] == 'error'):
            logging.error("!!!Error OpenStack cloud failed to create server " + server.__dict__["_info"].fault.message) 
            return None
            

        return OpenStackInstance(server)
        


    def makeVolumeFromImage(self , imageid , initialconfig, instancename):
        """generates cloud server instances from uploaded images"""
        image = self.__nova.images.get(imageid)
        
        # getting the image size. Primarily we rely on min_disk attribute we set when image is created
        size_gb = 0
        if image.min_disk:
            size_gb = image.min_disk
        else:
            if image._info.has_key('virtual_size') and int(image._info.has_key('virtual_size')):
                size = int(image._info['virtual_size'])
            if not size:
                if image._info.has_key('size') and int(image._info.has_key('size')):
                    size = int(image._info['size'])
        if not size_gb:
            size_gb = int((int(size)-1) / (1024*1024*1024)) + 1

        if self.__cinder:
            #TODO: somehow we should get the size
            volume = self.__cinder.volumes.create(size_gb , name = instancename, imageRef=imageid)
            return OpenStackCinderVolume(volume)
        else:
            volume = self.__nova.volumes.create(size_gb , name = instancename, imageRef=imageid)
            return OpenStackNovaVolume(self.__nova , volume)