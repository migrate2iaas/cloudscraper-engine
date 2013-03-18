import os
import sys
import shutil
import re
import subprocess

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

import logging
import EC2Instance
import time

class EC2InstanceGenerator(object):
    """generator class for ec2 instances"""

    def __init__(self , region, retries=5):

        self.__region = region
        self.__retryCount = retries


     # marks the data uploaded as system disk, should be called(?) after the upload is confirmed
     # TODO: make direct EC2 calls instead of this crappy java calls
    def makeInstanceFromImage(self , imageid , initialconfig, s3owner, s3key, temp_local_image_path ):

        windir = os.environ['windir']

        xml = imageid
        scripts_dir = ".\\Amazon"

        ec2region = self.__region
        machine_arch = initialconfig.getArch()
        ec2zone = initialconfig.getZone()
        tmp_vmdk_file = temp_local_image_path
      
        retry = 0
        # trying to get the import working for the several times
        while retry < self.__retryCount:
            retry = retry + 1

            proc_import_sys = subprocess.Popen([windir+'\\system32\\cmd.exe', '/C', scripts_dir+'\\import_vm.bat', tmp_vmdk_file, s3owner , s3key , xml, ec2zone , machine_arch , ec2region]
                  , bufsize=1024*1024*128, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            import_task_id = "";
            # waiting till the process completes        
            while proc_import_sys.poll() == None:
                # parse the output data from pipe here
                outdata = proc_import_sys.stdout.readline();
                # in case that is the progress line
                if "|--------------------------------------------------| 100" in outdata:
                    logging.debug("The upload of system volume status (this screen will be updated every 2% of image uploaded):");
                    #then read by byte and output
                    next_char = ' ';
                    completion = 0;
                    logging.debug(str(completion)+"% uploaded")
                    while next_char != '\n' and next_char != '':
                        next_char = proc_import_sys.stdout.read(1)
                        # 2 percents of completion per one = character in output
                        if next_char == '=':
                            completion = completion + 2
                            logging.debug(str(completion)+"% uploaded")
                #
                logging.debug(str(outdata))
                match = re.search('(import-i-[a-zA-Z0-9]+)',str(outdata))
                if match == None:
                    continue
                else:
                    logging.info("Conversion task = " + match.group(1));
                    import_task_id = match.group(1);
        

            (stdoutdata, stderrdata) = proc_import_sys.communicate();
            #in case there were some data left (e.g. the conversion was too fast)
            if stdoutdata and import_task_id == "":
                match = re.search('(import-i-[a-zA-Z0-9]+)',str(stdoutdata))
                if match == None:
                    logging.error("Couldn't get conversion task id for a system volume!");
                    return None
                else:
                    logging.info("Conversion task = " + match.group(1));
                    import_task_id = match.group(1);
                logging.debug(str(stdoutdata))


            if stderrdata:
                logging.error("!!!ERROR: Error occured while executing the system import process\n");
                logging.error(str(stderrdata))
                if stdoutdata:
                    logging.info("\nstdout:")
                    logging.info(str(stdoutdata))
                #retry if failed
                continue
            else:
                logging.info (">>>>>>>>>>>>>>> System volume has been uploaded, now it's converted by the Amazon EC2 to run as EC2 instance (it may take up to hour, be patient).")
                logging.info ("Waiting for system volume conversion to complete")
                #
                while 1:
                    if import_task_id == "":
                        logging.error("!!!ERROR: The conversion task number could not be got but import has been successful. Use your AWS console to attach data volumes to the new image manually upon their import and conversion complete");
                        return None
                
                    vms_output = subprocess.check_output([windir+'\\system32\\cmd.exe', '/C', scripts_dir+'\\check_vm.bat' , import_task_id, s3owner , s3key, ec2region])
                    logging.debug("-----Checked the system volume conversion state" + vms_output);
                    match = re.search('Status[ \n\t]*([a-zA-Z0-9]+)',vms_output)
                    if match == None:
                        logging.error("Error, couldn't parse the check_vm output: " + vms_output);
                        return None
                    importstatus = match.group(1)
                    if importstatus == "active" or importstatus == "pending":
                        match = re.search('Progress:[ \n\t]*([0-9]+)',vms_output)
                        if match:
                            logging.info("% Conversion Progress: " + match.group(1) + "%");
                        time.sleep(30) #30 sec
                        continue
                    if importstatus == "completed":
                        logging.info("Conversion done")
                        match = re.search('InstanceID[ \n\t]*(i-[a-zA-Z0-9]+)',vms_output)
                        if match == None:
                            logging.error("Error, couldn't parse the check_vm output: " + vms_output);
                            return None
                        instanceid = match.group(1)    
                        logging.info("==========================================================================");
                        logging.info(">>> The system instance " + instanceid + " has been successfully imported");
                        logging.info(">>> It could be configured and started via AWS EC2 management console");
                        logging.info("==========================================================================");

                        return EC2Instance.EC2Instance(instanceid)
                    if importstatus == "cancelled":
                        logging.error("!!!ERROR: The import task was cancelled by AWS, see full report for details...");
                        logging.error(vms_output)
                        return None

        return None