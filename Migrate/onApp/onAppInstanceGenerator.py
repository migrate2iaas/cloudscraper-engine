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
import time
import VmInstance
import socket

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
                    try:
                        response_data = str(response.read())
                        logging.error(str(response.status)+":"+ response_data)
                        data = json.loads(response_data);
                        logging.error("!!!ERROR: onApp cloud error: " + str(data["errors"]["base"]))
                    except Exception as e:
                        logging.error("!!!ERROR: cannot decode the error, please see logs")
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
            response = self.sendRequest("PUT", "/virtual_machines/"+str(vmid)+".json", request);
            
        def shutdownVM(self , vmid):
            response = self.sendRequest("POST", "/virtual_machines/"+str(vmid)+"/stop.json");

        def startVM(self, vmid):
            response = self.sendRequest("POST", "/virtual_machines/"+str(vmid)+"/startup.json");

        def deleteVM(self, vmid , convert_last_backup = 0 , destroy_all_backups = 0):
            response = self.sendRequest("DELETE", "/virtual_machines/"+str(vmid)+"/startup.json?convert_last_backup="+str(convert_last_backup)+"&destroy_all_backups="+str(destroy_all_backups));

        def getVM(self, vmid):
            response = self.sendRequest("GET", "/virtual_machines/"+str(vmid)+".json");
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


class onAppVM(VmInstance.VmInstance):
    """class representing onapp vm"""
    def __init__(self , onapp, vmid):
        self.__onapp = onapp
        self.__vmid = vmid
        

    def run(self):
        """starts instance"""
        vm = self.__onapp.getVM(self.__vmid)
        if vm['booted'] == True:
            return True
        self.__onapp.startVM()

    def stop(self):
        """stops instance"""
        vm = self.__onapp.getVM(self.__vmid)
        if vm['booted'] == False:
            return True
        self.__onapp.shutdownVM(self.__vmid)

    def attachDataVolume(self):
        """attach data volume"""
        raise NotImplementedError

    def getIp(self):
        """returns public ip string"""
        vm = self.__onapp.getVM(self.__vmid)
        ip = vm['ip_addresses'][0]['ip_address']['address']
        return ip

    def deallocate(self , subresources=True):
        """deallocates a VM
            Args:
            subresources: Boolean - if True, deallocates all associated resources (disks, ips). Deallocates only the vm itself otherwise
        """
        self.__onapp.deleteVM(self.__vmid, destroy_all_backups = int(subresources))

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
        """we just wait here to ensure disk attached"""
        logging.debug("Waiting till disk is attached")
        time.sleep(self.__diskWaitTimeout)
        return

    def initCreate(self , initialconfig):
        """inits the conversion"""
        # add parms like disk sizes here

        # TODO: get from parms
        
        return

    def launchMinipadServer(self):
        """ to implement in the inherited class """
        #here we should start minipad from template
        win_template_disk_size = 20
        #TODO: licensing should be configurable
        win_licensing_type = "mak"


        if not self.__minipadId and not self.__minipadTemplate:
            raise Exception("No minipad VM id or template set , cannot initialize minipad VM!")
        name = "Cloudscraper-Target"+str(long(time.time()))+"-VM"
        if self.__minipadTemplate and not self.__minipadId:
            logging.info("Launching template " + str(self.__minipadTemplate) + " to act as minipad target")
            vmParams = { "template_id" : int(self.__minipadTemplate) , "label" : name , "hostname" : name , "memory" : 2048 , "cpus" : 1 , "cpu_shares" : 1 , "primary_disk_size": win_template_disk_size , \
                "rate_limit" : 0 , "swap_disk_size" : 0 , "required_virtual_machine_build" : 1 , "required_ip_address_assignment" : 1 , "licensing_type": win_licensing_type}
            vm = self.__onapp.createVM(vmParams)
            ip = vm['ip_addresses'][0]['ip_address']['address']
            self.__minipadId = vm['identifier']
            return ip
        else:
            if self.__minipadId:
                logging.info("Using existing target " + str(self.__minipadId))
            
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

        
        vmParams = { "label" : name }
        self.__onapp.editVM(self.__minipadId, vmParams)
        vm = onAppVM(self.__onapp , self.__minipadId)
        vm.stop()

        return vm

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

    def waitTillVMBuilt(self , vmid, timeout=30*60):
        sleeptime = 60*1 # check every minute
        logging.info(">>> Waiting till onApp Cloudscraper VM is ready")
        while timeout > 0:
            vm = self.__onapp.getVM(vmid)
            logging.debug("VM status: " + str(vm))
            if vm["built"] == True and vm["locked"] == False:
                return
            timeout = timeout - sleeptime
            time.sleep(sleeptime)
        logging.error("!!!ERROR: Timeout, Cloudscraper target VM is not ready. Please contact the cloud provider.")
        return

    def __init__(self, onapp_endpoint , onapp_login , onapp_password , onapp_datastore_id, onapp_target_account = None, onapp_port = 80, preset_ip = None, minipad_image_id = "" , minipad_vm_id = None , vmbuild_timeout=100*60 ):
        """
        Args:
            onapp_endpoint - cloud endpoint address (ip or dns)
            onapp_login - login to onapp
            onapp_password - the password for onapp
            onapp_datastore_id - the id of datastore to create vms
            onapp_target_account - the account to transfer the resulting VM (if any)
            onapp_port - port of the endpoint
            preset_ip - set if the minipad VM is already launched
            minipad_image_id - the template id of minipad to launch
            minipad_vm_id - id if minipad is already created
            vmbuild_timeout: int - timeout in seconds to wait till target minipad VM is built
        """
        self.__onapp = OnAppBase();
        self.__onapp.connectOnApp(onapp_login, onapp_password, onapp_endpoint, str(onapp_port));
        

        version = self.__onapp.getVersion()
        logging.debug("onApp connected API version " + version)

        self.__diskSize = 100;
        self.__builtTimeOutSec = vmbuild_timeout;
        self.__diskWaitTimeout = 120 #2 mins
        self.__serviceStartTimeout = 120
        self.__minipadTemplate = minipad_image_id
        self.__minipadId = minipad_vm_id
        self.__datastore = onapp_datastore_id
        super(onAppInstanceGenerator, self).__init__(preset_ip)
        #TODO: should find datastore id via the label
        
    def startConversion(self,image , ip , import_type = 'ImportInstance' , server_port = 80):
        """override proxy. it waits till server is built and only then starts the conversion"""
        self.waitTillVMBuilt(self.__minipadId, timeout = self.__builtTimeOutSec )
        
        vm = onAppVM(self.__onapp, self.__minipadId)
        logging.info("Awaiting till Cloudscraper target VM is alive (echoing Cloudscraper VM RDP port)")
        if vm.checkAlive() == False:
            logging.warn("!Cloudscraper target VM is not repsonding (to RDP port). Misconfiguration is highly possible!")
        
        #extra wait for service availability
        time.sleep(self.__serviceStartTimeout)

        return super(onAppInstanceGenerator, self).startConversion(image, ip , import_type , server_port)


    def getDiskSize(self, imageid_manifest_link):
        """downloads image link to get the disk size"""
        imageid = imageid_manifest_link
        try:
            response = urllib2.urlopen(imageid)
            xmlheader = response.read()
            (head, sep ,tail) = xmlheader.partition("<volume-size>")
            if tail:
                    (head, sep ,tail) = tail.partition("</volume-size>")
                    self.__diskSize = int(head , base = 10) + 1 # add one so to fit 100%
                    logging.debug("The volume would be of size " + str(self.__diskSize) + " GBs")
            else:
                    logging.warning("!Couldn't parse the xml describing the import done")
        except Exception as e:
            logging.warning("!Cannot get XML manifest file from intermediate storage. Possibly the storage is inaccessible.")


    def makeInstanceFromImage(self , imageid, initialconfig, instancename):
        """makes instance based on image id - link to public image"""
        self.getDiskSize(imageid)
        return super(onAppInstanceGenerator, self).makeInstanceFromImage(imageid, initialconfig, instancename)

    def makeVolumeFromImage(self , imageid, initialconfig, instancename):
        """makes volume based on image id - link to public image"""
        self.getDiskSize(imageid)
        vm = onAppVM(self.__onapp, self.__minipadId)
        vm.run()
        volume = super(onAppInstanceGenerator, self).makeVolumeFromImage(imageid, initialconfig, instancename)
        vm.stop()
