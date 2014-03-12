"""
virtualmachine
~~~~~~~~~~~~~~~~~

This module provides virtualmachine class. 
Written in Azure Python SDK style
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import os
import time

from azure import *
from azure.servicemanagement import *

import string
from random import sample, choice

from win32com.client import Dispatch


class azure_mgmt_response(object):
    """response compatible with requests Response class"""

    def __init__(self , status , status_text , response_headers , response_body):
        self.status_code = status
        self.status = status_text
        self.headers = response_headers
        self.reason = response_body
        self.body = response_body

    def __repr__(self):
        return '<Response [%s]>' % (self.status)

    def __bool__(self):
        """Returns true if :attr:`status_code` is 'OK'."""
        return self.ok

    def __nonzero__(self):
        """Returns true if :attr:`status_code` is 'OK'."""
        return self.ok

    
    @property
    def ok(self):
        try:
            self.raise_for_status()
        except Exception:
            return False
        return True

    def raise_for_status(self):
        """Raises stored :class:`HTTPError`, if one occurred."""

        http_error_msg = ''

        if 400 <= self.status_code < 500:
            http_error_msg = '%s Client Error: %s' % (self.status_code, self.reason)

        elif 500 <= self.status_code < 600:
            http_error_msg = '%s Server Error: %s' % (self.status_code, self.reason)

        if http_error_msg:
            raise RuntimeError(http_error_msg)


# seems like no more need in this kind of certs, Python API has the appropriate calls (maybe implemented in the same way, not sure)
def send_cert_request(url, verb, body, cert_selection = "", parms = dict() , x_ms_version = "2012-03-01" , content_type="application/xml"):
    """
    Auxillary method to send HTTP requests thru Windows COM-library.

    Parms:
        url: str - url to send request to
        verb: str - HTTP verb like PUT or GET
        body: str - request bidy
        cert_selection: str - certificate selection string to sign ssl requests in form <location>\<store>\<cert-subject-name> like CURRENT_USER\My\azurecert 
        parms: dict - extra header params
        x_ms_version: str - api version string
        content_type: str - type of content in a body
    """

    logging.debug("Excuting WinHttpRequest " + verb + " " + url + " with cert " + cert_selection + " \nBody:\n" + body)
    req = Dispatch("WinHttp.WinHttpRequest.5.1")
    req.Open(verb, url)
    # It never checks if cert is valid! 
    # TODO: make some checks via other interface
    if cert_selection:
        retval = req.SetClientCertificate(cert_selection) # Store and user are taken by default if not specified in the same string
        if retval:
            logging.debug("SetClientCertificate() returned " + str(retval) + " on " + cert_selection)
    # seems like it'll be an buttheart in case we work in context of other user
    req.SetRequestHeader('x-ms-version', x_ms_version)
    req.SetRequestHeader("Content-Type", content_type)
    for (key, value) in parms.items():
        req.SetRequestHeader(key, value)
    req.Send(body)
    logging.debug("Returned " + str(req.Status) + " " + req.StatusText + "\n" + req.ResponseBody);
    response = azure_mgmt_response( req.Status, req.StatusText, req.GetAllResponseHeaders() ,  unicode(req.ResponseBody) )
    return response


class virtualmachine(object):
    """virtual machine service connection"""

    def __init__ (self , cert_selection, subscription_id, azure_url = "https://management.core.windows.net"):
        """
        
        Args:
            cert_selection: str - string to select management certificate in form <Location>\<store>\<cert-subject> or <cert-subj> for default location and store
            subscription_id: str - guid identifying an Azure subscription you are to manage
            azure_url: str - path to azure default url
        """

        self.__azureUrl = azure_url
        self.__subscription = subscription_id
        self.__baseUrl = azure_url+"/"+self.__subscription

        self.__certSelection = cert_selection

    def add_disk(self, disk_label , media_link , disk_name , os_type = "Windows"):
        """
        Adds disk to the user disk storage. 
        See http://msdn.microsoft.com/en-us/library/windowsazure/jj157178.aspx
        """
        operation = "/services/disks"
        verb = "POST"

        url = self.__baseUrl + operation

        xmlheader = '<Disk xmlns="http://schemas.microsoft.com/windowsazure" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">\n'
        xmlbody = '<OS>'+os_type+'</OS>\n'
        xmlbody = xmlbody + '<Label>'+disk_label+'</Label>\n'
        xmlbody = xmlbody + '<MediaLink>'+media_link+'</MediaLink>\n'
        xmlbody = xmlbody + '<Name>'+disk_name+'</Name>\n'
        
        xml = xmlheader+xmlbody+'</Disk>'

        return send_cert_request(url , verb , xml , self.__certSelection)

    def get_network_affinity_group(self, network):
        """
        Gets an affinitity group for a virtaal network
        """
        operation = "/services/networking/virtualnetwork"

        verb = "GET"

        url = self.__baseUrl + operation

        response = send_cert_request(url , verb , "" , self.__certSelection)

        logging.debug("Got affinity group: " + str(response.body));

        # this code is a bad way to find descent entry in xml but it should work
        ntworkpart = response.body.find('<Name>'+network+'</Name>')
        if ntworkpart == -1:
            logging.warning("! Cannot get affinity group for a network");
            return ""
        affinitystart = ntworkpart+response.body[ntworkpart:].find('<AffinityGroup>') 
        if affinitystart == -1:
            logging.warning("! Cannot get affinity group for a network");
            return ""
        affinitystart = affinitystart+len('<AffinityGroup>')
        affinityend = ntworkpart + response.body[ntworkpart:].find('</AffinityGroup>')
        group = response.body[affinitystart : affinityend]
        return group

    def create_vm(self , new_vm_name, region , disk_name , affinity_group = None , network = None, subnet = None):
        """
        Creates VM thru Python API
        Args:
            new_vm_name: str - new VM name, should be unique for the user, should only contain letters, alphanums and '-' in between
            region: str - region where to create the machine
            disk_name: str - os disk name
            affinity_group: str - name of affinity group if specified (it's got automatically if network is specified)
            network: str - virtual network to create VM
            subnet: str - subnet in the network. use it only if network was specified
        """
        sms = ServiceManagementService(self.__subscription, self.__certSelection)

       
        name = new_vm_name
        label = new_vm_name.replace("-","").replace(".","").replace(":","").replace("_","")
        location = region
        
        if network:
            affinity_group = self.get_network_affinity_group(network)
            logging.debug("Got affinity group " + affinity_group + " for Network " + network);

        service_name = self.getVmService(name)
        service_label = service_name.replace("-","").replace(".","").replace(":","").replace("_","")

        logging.info(">>>>>> Creating new cloud service " + service_name)
        #TODO: check this stuff
        if affinity_group:
            sms.create_hosted_service(service_name=service_name,
                label=service_label,
                affinity_group=affinity_group)
        else:
            # You can either set the location or an affinity_group
            sms.create_hosted_service(service_name=service_name,
                label=service_label,
                location=location)
        

        # Name of an os image as returned by list_os_images
        image_name = disk_name
        network_config = ConfigurationSet();
        network_config.configuration_set_type = "NetworkConfiguration"
        #TODO: should be customizable
        endpoint = ConfigurationSetInputEndpoint("RDP" , "TCP", "3389", "3389")
        network_config.input_endpoints.input_endpoints.append(endpoint)
        if subnet:
            network_config.subnet_names.append(subnet)
            

        os_hd = OSVirtualHardDisk(disk_name = disk_name , disk_label = disk_name)

        return sms.create_virtual_machine_deployment(service_name=service_name,
            deployment_name=service_name,
            deployment_slot='production',
            label=label,
            role_name=name,
            os_virtual_hard_disk=os_hd,
            system_config = None,
            network_config = network_config,
            role_size='Small', 
            virtual_network_name = network)

    def wait_vm_created(self , new_vm_name, timeout_sec = 1000 , recheck_interval = 5):
        """waits till new vm is created and running"""
        timeleft = timeout_sec
        while timeleft > 0:
            timeleft = timeleft - recheck_interval
            try:
                self.get_vm_info(new_vm_name)
                return True
            except Exception as e:
                 logging.debug("Failed to get the machine state")
                 logging.debug("Exception = " + str(e)) 
                 logging.debug(traceback.format_exc())
            time.sleep(recheck_interval)

        logging.warning("!Cannot wait fo new VM to switch to the running state. Timeout")
        return False


    def getVmService(self , vmname):
        """auxillary function to get service names for vms (in predefined way)"""
        service_name = vmname.replace("_","-")+"app"
        return service_name

    def start_vm(self, vmname):
        """starts vm role"""
        sms = ServiceManagementService(self.__subscription, self.__certSelection)
        sms.start_role(self.getVmService(vmname), self.getVmService(vmname), vmname)
        return True

    def stop_vm(self, vmname):
        """starts vm role"""
        sms = ServiceManagementService(self.__subscription, self.__certSelection)
        sms.shutdown_role(self.getVmService(vmname), self.getVmService(vmname), vmname)
        return True


    def get_vm_info(self, vmname):
        """gets vm role info"""
        sms = ServiceManagementService(self.__subscription, self.__certSelection)
        return sms.get_role(self.getVmService(vmname), self.getVmService(vmname), vmname)