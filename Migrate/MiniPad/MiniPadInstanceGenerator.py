
import sys

sys.path.append('./..')
sys.path.append('.')

sys.path.append('.\..\Windows')
sys.path.append('.\..\MiniPad')


sys.path.append('.\Windows')

sys.path.append('.\MiniPad')

import InstanceGenerator
import logging
import traceback
import requests
import time
from lxml import etree

class MiniPadInstanceGenerator(InstanceGenerator.InstanceGenerator):
    """Generator of instances via minipad. It's pretty similar to EC2 but has some of its own features aswell"""
   
    # to override
    
    def __init__(self , preset_ip = None , finalize_every_vol = False , postprocess = True):
        """
        Args:
            preset_ip - an ip address of running minipad instance (if any)
            finalize_every_vol - if to call for finalization after every one volume uploaded. Set this to False if Minipad runs on the same VM where resulting system is to run
            postprocess - whether to call for postprocessing (injecting drivers etc) after the instance import is complete
        """
        self.__server_ip = preset_ip
        self.__server_port = 80
        self.__finalizeEveryVolume = finalize_every_vol
        self.__postprocess = postprocess
        super(MiniPadInstanceGenerator, self).__init__()

    def createVM(self ,disk ,name):
        """ to implement in the inherited class 
        
        return server ip
        """
        return 

    def createDisk(self , name):
        """ to implement in the inherited class """
        return 

    def attachDiskToMinipad(self , disk):
        """ to implement in the inherited class """
        return

    def detachDiskFromMinipad(self , disk):
        """ to implement in the inherited class """
        return

    def launchMinipadServer(self):
        """ to implement in the inherited class """
        return

    def destroyMinipadServer(self):
        """ to implement in the inherited class """
        # TODO: add it to kinda destructor
        return

    def initCreate(self , initialconfig):
        """inits the process and local vars to process the conversion"""
        return

    # ------------- end overrides

    
    def __post(self, payload):
        url = "http://%s:%d/" % (self.__server_ip, self.__server_port)

        r = requests.post(url, data = payload)
        if r.status_code < 400:
            try:
                e = etree.fromstring(r.content)
                logging.debug(etree.tostring(e, pretty_print=True))
                return e
            except Exception as e:
                logging.warning("!Invalid output: " + r.content )
                logging.warning("The exception is " + str(e))
                # save log files to disk
                log = open('../../logs/minipad.log.tar.gz', 'wb')
                log.write(r.content)
                log.close()

                logging.debug('Log Received: saved as ../../logs/minipad.log.tar.gz')

                return None
        else:
            r.raise_for_status()

    def  __getLog(self):
        url = "http://%s:%d/" % (self.__server_ip, self.__server_port)
        payload = {'Action' : 'GetImportTargetLogs',}
        r = requests.post(url, data = payload)
        if r.status_code < 400:
            log = open('../../logs/minipad.log.tar.gz', 'wb')
            log.write(r.content)
            log.close()
        else:
            r.raise_for_status()
        

    def finalizeConversion(self , set_boot = True):
        """finalizes the conversion sending minipad the end signal"""
        make_boot = 'False'
        if set_boot:
            make_boot = 'True'
        payload = {'Action' : 'FinalizeConversion' , 'MakeBoot' : make_boot}
        self.__post(payload)


    def startConversion(self, manifesturl , server_ip, import_type = 'ImportInstance' , server_port = 80):
        """Converts the VM"""
         # reset the service into its initial state
        payload = {'Action' : 'Restart'}
        self.__post(payload)

        # configure instance
        payload = {'Action' : 'ConfigureImport',
                   'SameDriveMode' : 'False',
                   'UseBuiltInStorage' : 'False',
                   'Postproccess' : str(self.__postprocess),
                  }
        self.__post(payload)

        logging.info(">>> Connection estabilished, starting transferring data from intermediate storage")

        # wait for ConfigurInstance
        time.sleep(20)

        # get status
        payload = {'Action' : 'GetImportTargetStatus',}
        r = self.__post(payload)

        ## Import an Instance
        payload = {'Action' : import_type,
                   'Image.Format' : 'VMDK',
                   'Image.ImportManifestUrl' : manifesturl,
                  }
        self.__post(payload)

        done = False
        waited = 0
        while not done:
            # wait for 30 seconds
            delay = 30
            logging.debug("Waiting %d seconds..." % delay)
            time.sleep(delay)
            waited = waited + delay

            ## DescribeConversionTasks
            payload = {'Action' : 'DescribeConversionTasks',}
            self.__post(payload)

            # get status
            payload = {'Action' : 'GetImportTargetStatus',}
            r = self.__post(payload)

            # check status
            Status = r.find('Status')
            StatusMessage = r.find('StatusMessage')
            if Status.text in ['Error', 'FinishedTransfer']:
                done = True
            if waited % delay*10:
                logging.info("% " + StatusMessage.text)


        # get status
        payload = {'Action' : 'GetImportTargetStatus',}
        self.__post(payload)

        # get log file
        self.__getLog()

        
        # finalizeimport
        if self.__finalizeEveryVolume:
            self.finalizeConversion(import_type == 'ImportInstance')

        # get status
        payload = {'Action' : 'GetImportTargetStatus',}
        status = self.__post(payload)
        
        if int(status.find("StatusCode").text) >= 400:
            logging.error("!!!ERROR: Bad import status " + status.find("StatusMessage").text)
            return False

        return True

    def makeInstanceFromImage(self , imageid, initialconfig, instancename):
        """generates cloud server instances from uploaded images

        Args:
            imageid - str, is url of manifest file
            initialconfig - CloudConfig.CloudConfig
            instancename - resulting cloud instance name
        """
        self.initCreate(initialconfig)
        ip = self.launchMinipadServer()
        if ip:
            self.__server_ip = ip
        disk = self.createDisk(instancename)
        self.attachDiskToMinipad(disk )
        
        if self.startConversion(imageid , self.__server_ip) == False:
            return None

        self.detachDiskFromMinipad(disk)
        vm = self.createVM(disk , instancename)
        return vm

    def makeVolumeFromImage(self , imageid , initialconfig, instancename):
        """generates cloud server instances from uploaded images"""
        self.initCreate(initialconfig)
        disk = self.createDisk(instancename)
        self.attachDiskToMinipad(disk )
        
        if self.startConversion(imageid , self.__server_ip , "ImportVolume") == False:
            return None

        self.detachDiskFromMinipad(disk)
        return True
         #TODO: create volume object here