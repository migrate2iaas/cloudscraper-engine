"""
onAppInstanceGenerator
~~~~~~~~~~~~~~~~~

This module provides onAppInstanceGenerator class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback

import MiniPadInstanceGenerator
import onApp


import base64, httplib, urllib, urllib2, json, random, string, time, uuid, logging, os, sys;

class OnAppBase:
        conn = False;
        basicAuth = "";
        def connectOnApp(self, username, password, hostname, port):
                self.basicAuth = base64.encodestring('%s:%s' % (username, password)).replace('\n', '');
                self.conn = httplib.HTTPConnection(hostname, port);
                try:
                        logging.info("Connecting to HTTPConnection");
                        self.conn.connect();
                except:
                        logging.error('!!!ERROR: Unable to connect to ' + hostname  + '  HTTPConnection Connect, unable to continue!');

        def sendRequest(self, type, page, requestData=False):
                headers = {"Authorization": "Basic %s" % self.basicAuth, 'Accept': 'application/json', 'Content-type': 'application/json'};
                if requestData == False:
                        self.conn.request(type, page, None, headers);
                else:
                        self.conn.request(type, page, requestData, headers);
                response = self.conn.getresponse()
                if response.status >= 400:
                    logging.error("!!!ERROR: Http request " + str(type) + " " + str(page) + " to onApp cloud failed!")
                    logging.error(str(response.status)+":"+ str(response.read()))
                    raise IOError("Http operation failed!")
                return response


        def getVersion(self):
                #Make Request to OnApp with Basic Auth
                response = self.sendRequest("GET", "/version.json");
                array = json.loads(response.read());
                if 'version' in array:
                        return array['version'];
                return False;

        # should split into several classes if needed
        def createVM(self, vmParams):
            request = json.dumps({"virtual_machine": vmParams});
            response = self.sendRequest("POST", "/virtual_machines.json", request);
            data = json.loads(response.read());
            if 'virtual_machine' in data:
                return data['virtual_machine'];
            else:
                return data;

         # should split into several classes if needed
        def editVM(self, vmid, vmParams):
            request = json.dumps({"virtual_machine": vmParams});
            response = self.sendRequest("POST", "/virtual_machines/"+str(vmid)+".json", request);
            data = json.loads(response.read());
            if 'virtual_machine' in data:
                return data['virtual_machine'];
            else:
                return data;


        def createDisk(self , vmid , params):
            request = json.dumps({"disk": params});
            response = self.sendRequest("POST", "/virtual_machines/"+str(vmid)+"/disks.json", request);
            data = json.loads(response.read());
            if 'disk' in data:
                return data['disk'];
            else:
                return data;

            return

        def backupDisk(self , diskid):
            #TODO: analyze responses
            response = self.sendRequest("POST", "/settings/disks/"+str(diskid)+"/backups.json");
            data = json.loads(response.read());
            if 'backups' in data:
                return data['backups'];
            else:
                return data;

            return

        def createTemplate(self , backup_id , label, disksize, min_memory_size=512):
            params = {"label" : label , "min_disk_size" : disksize , "min_memory_size" : min_memory_size}
            request = json.dumps({"disk": params});
            response = self.sendRequest("POST", "/settings/disks/"+str(backup_id)+"/convert.json");
            data = json.loads(response.read());
            if 'image_template' in data:
                return data['image_template'];
            else:
                return data;
            return

        def backupVM(self , vmid):
            response = self.sendRequest("POST", "/virtual_machines/"+str(vmid)+"/backups.json");
            data = json.loads(response.read());
            if 'backup' in data:
                return data['backup'];
            else:
                return data;

            return

class onAppInstanceGenerator(MiniPadInstanceGenerator.MiniPadInstanceGenerator):
    """on app generator"""

    def createDisk(self , name):
        size = 100;
        parms = {"label":name , "disk_size" : self.__diskSize , "data_store_id" : int(self.__datastore) , "hot_attach" : 1}
        disk_out = self.__onapp.createDisk(self.__minipadId , parms)
        logging.debug(repr(disk_out))
        #self.__diskId = 
        return disk_out['id']

    def attachDiskToMinipad(self, disk):

        return

    def initCreate(self , initialconfig):
        """inits the conversion"""
        # add parms like disk sizes here

        # TODO: get from parms
        self.__diskSize = 100;
        return

    def launchMinipadServer(self):
        """ to implement in the inherited class """
        #here we should start minipad from template
        return

    def destroyMinipadServer(self):
        """ to implement in the inherited class """
        # TODO: add it to kinda destructor
        return

    def createVM(self ,disk ,name):
        """ to implement in the inherited class 
        
        return server ip
        """
        #TODO: customize VM size
        # Here we should customize cloudscraper minipad image

        #vmParams = { "template_id" : self.__templateId , "label" : name , "hostname" : name , "memory" : 1024 , "cpus" : 1 , "cpu_shares" : 1 , primary_disk_size : self.__diskSize}
        #self.__onapp.createVM(vmParams)
        vmParams = { "label" : name }
        self.__onapp.editVM(self.__minipadId, vmParams)

        
        #TODO return object of VM type
        return 

    def detachDiskFromMinipad(self , disk):
        """ to implement in the inherited class """
        #Create Disk Backup  https://docs.onapp.com/display/31API/Create+Disk+Backup
        #Convert Backup to Template https://docs.onapp.com/display/31API/Convert+Backup+to+Template
               
        #backups = self.__onapp.backupVM(self.__minipadId)#self.__onapp.backupDisk(disk)
        #if len(backups) == 0:
        #    logging.error("!!!ERROR: disk template creation failed (backup failed)")
        #for backup in backups:
            # assume the last one is ours (it should be one of them all the times, by the way)
        #    bu_id = backup['id']
        #template_id = self.__onapp.createTemplate(self)['id']
        #self.__templateId = template_id
        return

    def __init__(self, onapp_endpoint , onapp_login , onapp_password , onapp_datastore_id, onapp_target_account = None, onapp_port = 80, preset_ip = None, minipad_image_name = "" , minipad_vm_id = None):
        """
        Args:
            onapp_endpoint - cloud endpoint address (ip or dns)
            onapp_login - login to onapp
            onapp_password - the password for onapp
            onapp_datastore_id - the id of datastore to create vms
            onapp_target_account - the account to transfer the resulting VM (if any)
            onapp_port - port of the endpoint
            preset_ip - set if the minipad VM is already launched
            minipad_image_name - the image name of minipad to launch
            minipad_vm_id - id if minipad is already created
        """
        self.__onapp = OnAppBase();
        self.__onapp.connectOnApp(onapp_login, onapp_password, onapp_endpoint, str(onapp_port));
        
        version = self.__onapp.getVersion()
        logging.debug("onApp connected API version " + version)

        self.__minipadId = minipad_vm_id
        self.__datastore = onapp_datastore_id
        super(onAppInstanceGenerator, self).__init__(preset_ip)
        #TODO: should find datastore id via the label
        
    


    def makeInstanceFromImage(self , imageid, initialconfig, instancename):
        return super(onAppInstanceGenerator, self).makeInstanceFromImage(imageid, initialconfig, instancename)
