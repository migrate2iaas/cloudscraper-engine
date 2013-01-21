import BackupAdjust
import os
import sys
import shutil

import win32api
import win32con
import win32security
import win32file

import struct

import logging

class WindowsBackupAdjust(BackupAdjust.BackupAdjust):
    """Backupa adjust for Windows OS"""


    def __init__(self, adjustConfig):
        self.__adjustConfig = adjustConfig
        return super(WindowsBackupAdjust,self).__init__()        

    def adjustSystemHive(self ,hiveFilePath):
        
        logging.info("Adjusting the system hive");

        originalwindir = os.environ['windir']
        windrive = originalwindir.split("\\")[0] #get C: substring

        # 1) mount hive from path
        hivekeyname = "MigrationSys"
        
        # A call to RegLoadKey fails if the calling process does not have the SE_RESTORE_PRIVILEGE privilege.
        token = win32security.OpenProcessToken(-1, win32con.TOKEN_ALL_ACCESS )
        privid = win32security.LookupPrivilegeValue(None, "SeRestorePrivilege" )
        privilages = win32security.AdjustTokenPrivileges(token, False , [(privid , win32security.SE_PRIVILEGE_ENABLED)])

        win32api.RegLoadKey(win32con.HKEY_LOCAL_MACHINE, hivekeyname, hiveFilePath)

        # 2) alter it: change keys (they should be in the config: some of keys should be added, some should be deleted)

        #To change: Mountdevs, driver entries , maybe setups, system\boot partitions
        #optionally: add my soft

        

        # 2a) mountdevs
        mountdevkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\MountedDevices" , 0 , win32con.KEY_ALL_ACCESS )
        (oldvalue,valtype) = win32api.RegQueryValueEx(mountdevkey, "\\DosDevices\\"+windrive)
        newvalue =  struct.pack('=i',self.__adjustConfig.getNewMbrId()) + struct.pack('=q',self.__adjustConfig.getNewSysPartStart()) 
        win32api.RegSetValueEx(mountdevkey, "\\DosDevices\\"+windrive , 0, valtype, newvalue)
        #TODO: replace mountpoints for another vols\VolGuids too
        mountdevkey.close()

        logging.debug("Set the mountpoint for\\DosDevices\\"+windrive + " from " + str(oldvalue) + "to " + str(newvalue));

        # 2b) change firmware\system entires in \\CurrentControlSet\Control
        selectkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\Select" , 0 , win32con.KEY_READ )
        (currentcontrolset,valtype) = win32api.RegQueryValueEx(selectkey, "Current")
        selectkey.close()

        logging.debug("Current Control Set is " + str(currentcontrolset));

        
        #let's turn all of them by default
        diskSCSI = True
        diskIDE = True

        if self.__adjustConfig.getSysDiskType() == self.__adjustConfig.diskScsi:
            diskSCSI = True
            diskIDE = False
        if self.__adjustConfig.getSysDiskType() == self.__adjustConfig.diskAta:
            diskSCSI = False
            diskIDE = True
        

        if diskSCSI:
            logging.info("Setting the SCSI driver as default");

            controlsetkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Control" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(controlsetkey, "SystemBootDevice" , 0, win32con.REG_SZ, "scsi(0)disk(0)rdisk(0)partition(1)")
            win32api.RegSetValueEx(controlsetkey, "FirmwareBootDevice" , 0, win32con.REG_SZ, "scsi(0)disk(0)rdisk(0)partition(1)")
            controlsetkey.close()

            # 2c) add lsiscsi to load on start
            lsikey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\LSI_SCSI" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(lsikey, "Start" , 0, win32con.REG_DWORD, 0)
            lsikey.close()
        
        if diskIDE:
            logging.info("Setting the IDE driver as default");

            controlsetkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Control" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(controlsetkey, "SystemBootDevice" , 0, win32con.REG_SZ, "multi(0)disk(0)rdisk(0)partition(1)")
            win32api.RegSetValueEx(controlsetkey, "FirmwareBootDevice" , 0, win32con.REG_SZ, "multi(0)disk(0)rdisk(0)partition(1)")
            controlsetkey.close()

            # 2c) add intelide\atapi to load on start
            idekey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\intelide" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(idekey, "Start" , 0, win32con.REG_DWORD, 0)
            idekey.close()

            atapikey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\atapi" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(atapikey, "Start" , 0, win32con.REG_DWORD, 0)
            atapikey.close()


        turnRDP = True
    
        if turnRDP:
            logging.info("Turning on RDP");

            #NOTE: checked on 6.1 only! 
            #TODO: Should recheck on other versions!
            # 2d) add turn on rdp feature
            firewarllruleskeypath = hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\SharedAccess\\Parameters\\FirewallPolicy\\FirewallRules"
            logging.debug("Openning key" + firewarllruleskeypath);
            firewallkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, firewarllruleskeypath , 0 , win32con.KEY_ALL_ACCESS )

            remotedesk_value = "RemoteDesktop-In-TCP"
            (oldvalue,valtype) = win32api.RegQueryValueEx(firewallkey, remotedesk_value)
            logging.debug("Got " + remotedesk_value + " = " + str(oldvalue));
            newvalue1 = str(oldvalue).replace("Active=FALSE", "Active=TRUE");
            newvalue2 = newvalue1.replace("Action=Block", "Action=Allow")
            logging.debug("Changing to  " + newvalue2)
            win32api.RegSetValueEx(firewallkey, remotedesk_value , 0 , win32con.REG_SZ, newvalue1)
            firewallkey.close()

            #2e) setting the rdp setting to ones needed
            #TODO: make kinda wrapper function!
            terminalkeypath = hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Control\\Terminal Server\\WinStations\\RDP-TCP"
            logging.debug("Openning key" + terminalkeypath);
            terminalkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, terminalkeypath , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(terminalkey, "UserAuthentication" , 0, win32con.REG_DWORD, 1)
            win32api.RegSetValueEx(terminalkey, "SecurityLayer" , 0, win32con.REG_DWORD, 1)
            win32api.RegSetValueEx(terminalkey, "fAllowSecProtocolNegotiation" , 0, win32con.REG_DWORD, 1)
            terminalkey.close()

        turnHyperV = True
    
        if turnHyperV:
            logging.info("Turning on HyperV bus");

            #Note: it is good for 6.0+ only
            idekey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\vmbus" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(idekey, "Start" , 0, win32con.REG_DWORD, 0)
            idekey.close()
        #Do operations
        #for operation in self.__adjustConfig.getSystemHiveOperations():
        #    operation.Do(rootRegKey)

        # 3) dismount
        win32api.RegUnLoadKey(win32con.HKEY_LOCAL_MACHINE, hivekeyname)

        # 4) check the hive has the same size it was before

        # 5) maybe find a way to shrink it?
        return

    def adjustSoftwareHive(self ,hiveFilePath):
        
        # 1) mount hive from path

        # 2) alter it: change keys (they should be in the config: some of keys should be added, some should be deleted)

        #To change: rdp settings, dhcp settings, firewall settings

        #Do operations
        #for operation in self.__adjustConfig.getSoftwareHiveOperations():
        #    operation.Do(rootRegKey)

        # 3) dismount

        # 4) check the hive has the same size it was before

        # 5) maybe find a way to shrink it?
        return

    # creates and adjusts new BCD registry hive returning path to it
    def generateBcd(self, backupSource):
        # we use pregenerated hive which we alter a bit in here
        originalhivepath = self.__adjustConfig.getPregeneratedBcdHivePath()
        hivepath = os.environ['TEMP']+"\\tempbcdhive"
        shutil.copy2(originalhivepath , hivepath)
        #TODO: copy hive to sorta temp dir
        bcdkeyname = "MigrationBCD"
        
        logging.info("Creating new BCD record");

        # A call to RegLoadKey fails if the calling process does not have the SE_RESTORE_PRIVILEGE privilege.
        token = win32security.OpenProcessToken(-1, win32con.TOKEN_ALL_ACCESS )
        privid = win32security.LookupPrivilegeValue(None, "SeRestorePrivilege" )
        privilages = win32security.AdjustTokenPrivileges(token, False , [(privid , win32security.SE_PRIVILEGE_ENABLED)])

        win32api.RegLoadKey(win32con.HKEY_LOCAL_MACHINE, bcdkeyname, hivepath)
        logging.debug("Openning " + bcdkeyname+"\\Objects" );
        objectskey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, bcdkeyname+"\\Objects" , 0 , win32con.KEY_ALL_ACCESS )
        guidskeys = win32api.RegEnumKeyEx(objectskey)
        for  (keyname, reserved, classname, modtime) in guidskeys:
            elementskey = win32api.RegOpenKeyEx(objectskey, keyname+"\\Elements" , 0 , win32con.KEY_ALL_ACCESS )
            valuekeys = win32api.RegEnumKeyEx(elementskey)
            for  (valkeyname, reserved, classname1, modtime1) in valuekeys:
                if valkeyname == "11000001" or valkeyname == "21000001":
                    logging.debug("Changing the element " + valkeyname + " of " + keyname + " bcd object" );

                    valuekey = win32api.RegOpenKeyEx(elementskey, valkeyname , 0 , win32con.KEY_ALL_ACCESS )
                    (value,valtype) = win32api.RegQueryValueEx(valuekey, "Element")
                    if valtype != win32con.REG_BINARY:
                        #TODO: err here
                        return
                    newvalue_part1 = value[0:0x20] + struct.pack('=q',self.__adjustConfig.getNewSysPartStart()) 
                    newvalue = newvalue_part1 + value[0x28:0x38] + struct.pack('=i',self.__adjustConfig.getNewMbrId()) + value[0x3c:];
                    logging.debug("From " + value + "to " + newvalue);
                    logging.debug("MBR ID = " + hex(self.__adjustConfig.getNewMbrId()) + " , PartOffset = " + hex(self.__adjustConfig.getNewMbrId()))
                    win32api.RegSetValueEx(valuekey, "Element", 0, valtype, newvalue)
                    valuekey.close()
            elementskey.close()
        objectskey.close()

        win32api.RegUnLoadKey(win32con.HKEY_LOCAL_MACHINE , bcdkeyname)
        return hivepath
        # TODO: set original security access to the hive afterwards
                
      
    # creates and adjusts 
    def adjustBcd(self, backupSource):
        originalwindir = os.environ['windir']
        windrive = originalwindir.split("\\")[0] #get C: substring
        bootdir = windrive+"\\Boot"
        
        newbcd = self.generateBcd(backupSource)

        #TODO: note we may need some cleanup management after all theese copies
        logging.info("Adjusting BCD settings");
        if (os.path.exists(bootdir) and os.path.exists(bootdir+"\\BCD")):
            #found it on system drive, we should alter it somehow, easy
            logging.debug("BCD found, making replacement for it");
            sizedelta = int(os.path.getsize(bootdir+"\\BCD") - os.path.getsize(newbcd))
            if sizedelta > 0:
                logging.debug("New bcd is larger " + str(sizedelta) + " bytes than the new generated one");
                logging.debug("Appending nulls");
                #pad new bcd with 0 to fit the original size
                bcdfile = open(newbcd, "a+b")
                nulls = bytearray(sizedelta) # is filled with nulls by python
                bcdfile.write(nulls)
            if sizedelta < 0:
                logging.error("Cannot insert the new BCD because it's larger than the original one");
                return 

            self.replaceFile("\\Boot\\BCD" , newbcd)
            return
        else:
            logging.error("BCD was not found on the original location!");
            return
            
    def adjustHal(self, backupSource):
        fiierepopath = originalwindir+"\\System32\\DriverStore\\FileRepository"
        #Note: it's better to analyze BackupSource! 
        #Note: It's not good assumption: Windows BackupSource may be not local
        drvstore = os.listdir(fiierepopath) 
        #self.BackupAdjust.replaceFile(windir+"\\system32\\hal.dll" , originalwindir+"\\system32\\halmacpi.dll")

        #NOTE: we need to adjust it only on 2003. On Win 2008 we may just choose HAL options in BCD, (kernel is already MP there)
        # or it easier just to use /detecthal option in the boot loader 

        # so this code is needed only in case of unpacking *cab files from win2003
        for dirname in drvstore:
            if "hal.inf" in dirname:
                #We use halmacpi (multiprocessor variant) by default
                halpath = fiierepopath+dirname+"\\halmacpi.dll"
                if os.path.exists(halpath):
                    #NOTE: we must install hal of correct SP of windows, older SPs are in this list too
                     self.replaceFile(windir+"\\system32\\hal.dll" , halpath)
 

    def adjustRegistry(self , backupSource):

        originalwindir = os.environ['windir']
        windir = originalwindir.split(":\\")[-1] #get substring after X:\
        tmphiveroot = os.environ['TEMP']
        #TODO: more logs here
        sys_hive_path = tmphiveroot+"\\syshive";
        sysHiveTmp = open(sys_hive_path, "wb")
        extentsSysHive = backupSource.getFileBlockRange(windir+"\\system32\\config\\system")
        for extent in extentsSysHive:
            data_read = extent.getData()
            data_read_size = len(data_read)
            sysHiveTmp.write(data_read)

        sysHiveTmp.close();

        self.adjustSystemHive(sys_hive_path)

        extentsSoftHive = backupSource.getFileBlockRange(windir+"\\system32\\config\\software")
        soft_hive_path = tmphiveroot+"\\softhive";
        softHiveTmp = open(soft_hive_path, "wb")
        for extent in extentsSoftHive:
            data_read = extent.getData()
            data_read_size = len(data_read)
            softHiveTmp.write(data_read)

        self.adjustSoftwareHive(soft_hive_path)

        # We replace the file to the file of same size etc
        self.replaceFile(windir+"\\system32\\config\\system" , sys_hive_path)
        self.replaceFile(windir+"\\system32\\config\\software" , soft_hive_path)


    def configureBackupAdjust(self , backupSource):
        

        #Note: we may use adjust config here too 
        # now it's hardcoded

        # 1) Exclude files not needed
        # TODO: find all the pagefiles
        self.removeFile("pagefile.sys")
        self.removeFile("hiberfil.sys")

        # 1) Replace files we gonna change
        #TODO: add auto-add data to an any extent we read

        self.adjustRegistry(backupSource)

        
        #NOTE: we shall use it only if HAL needed to be changed
        #adjustHal() not needed right now
        
        # Add boot files (add files from boot\BCD)
        self.adjustBcd(backupSource)

        
        
