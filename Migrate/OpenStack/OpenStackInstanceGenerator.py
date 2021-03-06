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

import VmInstance
import VmVolume


class OpenStackInstance(VmInstance.VmInstance):
    """OpenStack instance"""

    def __init__(self, server, public_ip = None):
        super(OpenStackInstance , self).__init__(server.id)
        self.__server = server
        self.__ip = public_ip

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
        if self.__ip:
            return self.__ip

        for (network_name,network) in self.__server.addresses.iteritems():
            logging.info("Server connected to the network " + str(network_name))
            for addr in network:
                logging.info("Found ip address for the server: " + addr['addr'])
                if addr['OS-EXT-IPS:type'] == "floating":
                    return str(addr['addr'])
            

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

class OpenStackCinderVolume(VmVolume.VmVolume):
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

    def __init__(self , server_url , tennant_name , username , password , vmbuild_timeout_sec = 1200 , version="2" , ignore_ssl_cert = True , ip_pool = ""):
        self.__server_url = server_url
        self.__tennant = tennant_name
        self.__username = username
        self.__password = password
        self.__ignoreSslCert = ignore_ssl_cert
        self.__ipPool = ip_pool
        #keystone = ksclient.Client(auth_url=server_url,   username=username, password=password, tenant_name= tennant_name)
        #self.__auth = keystone.auth_token
        self.__nova = client.Client(version,username,password,tennant_name,server_url,insecure=self.__ignoreSslCert)
        self.__nova.authenticate()

        try:
            from cinderclient.v2 import client as ciclient
            self.__cinder = ciclient.Client(username=username,api_key=password,project_id=tennant_name,auth_url=server_url,service_type='volume',insecure=self.__ignoreSslCert)
            self.__cinder.authenticate()
        except Exception as e:
            self.__cinder = None
            logging.warning("!Cinder block storage is unavailable");
            logging.warning(traceback.format_exc())

        self.__vmbuild_timeout_sec = int(vmbuild_timeout_sec)
        super(OpenStackInstanceGenerator, self).__init__()

    def __findFlavor(self, initialconfig , image):
        """Finds matching flavor dependiong on configuration and disk size"""
        flavors = self.__nova.flavors.list()
        # picking the default if no config specified
        flavor = flavors[0]
        flavor_name = ""
        flavor_disk = None
        
        if initialconfig:
           if initialconfig.getInstanceType():
               flavor_name = initialconfig.getInstanceType()

        if int(image.minDisk) > int(flavor.disk):
            flavor_disk = int(image.minDisk)
            if flavor_name:
                logging.warn("! The selected flavor type is not enough to store the system data. Seeking for flavor possesing enough disk space")

        if flavor_disk or flavor_name:
               logging.info("Seeking for flavor named " + flavor_name + " and size at least " + str(int(image.minDisk)) + " GB")
               found = None
               for flv in flavors:
                   logging.debug("Found flavor " + repr(flv.__dict__))
                   if flavor_name and flv.name == flavor_name:
                       found = flv
                       break
                   # searching matching disk if machine is not foind
                   elif flavor_disk:
                      if flavor_disk < int(flv.disk):
                          found = flv
                          break

               if not found:
                   logging.warn("! VM instancr flavor (size) not found. Using default one: " + flavor.name)
               else:
                   flavor = found
                   logging.info("Machine flavor will be: " + flavor.name)

        return flavor


    def makeInstanceFromImage(self , imageid, initialconfig, instancename):
        """generates cloud server instances from uploaded images"""

        # just some debug output
        logging.debug("Servers found:")
        servers =self.__nova.servers.list()
        for server in servers:
            logging.debug(str(server.__dict__))

        images = self.__nova.images.list()
        logging.debug("Images found:")
        for image in images:
            logging.debug(str(image.__dict__))
        
        image = self.__nova.images.get(imageid)

        flavor = self.__findFlavor(initialconfig , image)

        # prepare network
        nics = None

        network_name = "network-for-az1"
        if initialconfig:
           if initialconfig.getSubnet():
               network_name = initialconfig.getSubnet()

        networks = self.__nova.networks.list()
        for network in networks:
            logging.info(str(network.__dict__))
            if str(network.label) == str(network_name):
                nic = dict()
                nic['net-id'] = network.id
                nics = list()
                nics.append(nic)
                break
        
        if not nics:
            logging.warning("!Network with label \'" + network_name +  "\' has not been found")

        # get availability zone
        az = None
        if initialconfig:
           if initialconfig.getZone():
               az = initialconfig.getZone()

        # create server
        logging.info(">>> Creating new server from image " + imageid)
        server = self.__nova.servers.create(instancename , image , flavor=flavor, nics = nics , availability_zone = az )

        #wait for server to be created
        waited = 0
        interval_to_wait = self.__vmbuild_timeout_sec / 30 + 1
        while True:
            time.sleep(interval_to_wait)
            waited = waited + interval_to_wait
            if (waited > self.__vmbuild_timeout_sec):
                logging.warning("! Server is still building.")
            servers = self.__nova.servers.list()
            for upd_server in servers:
                 if server.id == upd_server.id:
                     logging.debug(repr(server.__dict__))
                     server = upd_server
            if not server.__dict__['OS-EXT-STS:vm_state'] == "building":
                # sometimes this parm is not set just when build is requested
                break
        
        logging.info(">>> New server in " + server.__dict__['OS-EXT-STS:vm_state'] + " state " )
        logging.info(str(server.__dict__))
        if (server.__dict__['OS-EXT-STS:vm_state'] == 'error'):
            logging.error("!!!Error OpenStack cloud failed to create server " + server.__dict__["_info"]["fault"]["message"]) 
            return None
            
        # configure outbound network
        logging.info(">>> Enabling external network")
        
        external_ip = None
        try:
            #TODO: pass the pool via config (e.g. like availablilty zone?)
            pools = self.__nova.floating_ip_pools.list()
            ip_pool = pools[0]
            
            if self.__ipPool:
                pool_name = self.__ipPool
                # we treat zone name as pool name. Maybe there is a way to interrelate them. Not sure for now
                logging.debug("IP pools found:")
                for pool in pools:
                    logging.debug(str(pool.__dict__))
                    if pool.name == pool_name:
                        ip_pool = pool
                        logging.info("Found ip pool by name " + pool.name)

            logging.info("Allocating new external IP from pool " + ip_pool.name)
            new_ip = self.__nova.floating_ips.create(ip_pool.name)
            server.add_floating_ip(new_ip)
            external_ip = new_ip.ip
        except Exception as e:
            logging.warn("! Unable to add external ip to the server! Please, contact your cloud support")

        return OpenStackInstance(server , external_ip)
        


    def makeVolumeFromImage(self , imageid , initialconfig, instancename):
        """generates cloud server instances from uploaded images"""
        image = self.__nova.images.get(imageid)
        
        # getting the image size. Primarily we rely on min_disk attribute we set when image is created
        size_gb = 0
        size = 0
        if image.minDisk:
            size_gb = image.minDisk
        else:
            if image._info.has_key('virtual_size') and int(image._info.has_key('virtual_size')):
                size = int(image._info['virtual_size'])
            if not size:
                if image._info.has_key('size') and int(image._info.has_key('size')):
                    size = int(image._info['size'])
            if not size:
                if image.__dict__.has_key('OS-EXT-IMG-SIZE:size'):
                    size = int(image.__dict__.has_key('OS-EXT-IMG-SIZE:size'))
      
        if not size_gb:
            if not size:
                logging.error("!!!ERROR: cannot get the image size: " + repr(image) + " " + repr(image.__dict__))
            else:
                size_gb = int((int(size)-1) / (1024*1024*1024)) + 1

        if self.__cinder:
            #TODO: somehow we should get the size
            volume = self.__cinder.volumes.create(size_gb , name = instancename, imageRef=imageid)
            return OpenStackCinderVolume(volume)
        else:
            volume = self.__nova.volumes.create(size_gb , display_name = instancename, imageRef=imageid)
            return OpenStackNovaVolume(self.__nova , volume)