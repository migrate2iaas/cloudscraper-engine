# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

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
import time

class WindowsBackupAdjust(BackupAdjust.BackupAdjust):
    """Backupa adjust for Windows OS"""


    def __init__(self, adjust_config , windows_version):
        self.__adjustConfig = adjust_config
        self.__windowsVersion = windows_version
        return super(WindowsBackupAdjust,self).__init__()        

    def enableRdpFirewall(self , hivekeyname , currentcontrolset):
        if self.__windowsVersion > WindowsSystemInfo.WindowsSystemInfo.Win2003:
            firewarllruleskeypath = hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\SharedAccess\\Parameters\\FirewallPolicy\\FirewallRules"
            logging.debug("Openning key" + firewarllruleskeypath) 
            firewallkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, firewarllruleskeypath , 0 , win32con.KEY_ALL_ACCESS )

            try:
                remotedesk_value = "RemoteDesktop-In-TCP"
                (oldvalue,valtype) = win32api.RegQueryValueEx(firewallkey, remotedesk_value)
            except Exception as ex: 
                logging.debug("failed to find " + remotedesk_value + "in firewall settings. Possibly 2012 Server, trying RemoteDesktop-UserMode-In-TCP") 
                #TODO: ugly , should depend on win version
                remotedesk_value = "RemoteDesktop-UserMode-In-TCP"
                (oldvalue,valtype) = win32api.RegQueryValueEx(firewallkey, remotedesk_value)

            logging.debug("Got " + remotedesk_value + " = " + str(oldvalue)) 
            newvalue = str(oldvalue).replace("Active=FALSE", "Active=TRUE") 
            newvalue = newvalue.replace("Action=Block", "Action=Allow")
            
            portstart = newvalue.find("LPort=")
            portend = newvalue[newvalue.find("LPort="):].find("|")
            portentry = newvalue[portstart:portstart+portend]
            newvalue = newvalue.replace(portentry, "LPort="+str(rdpport))

            logging.debug("Changing to  " + newvalue)
            win32api.RegSetValueEx(firewallkey, remotedesk_value , 0 , win32con.REG_SZ, newvalue)
            firewallkey.close()
        else:
            firewarllruleskeypath = hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\SharedAccess\\Parameters\\FirewallPolicy\\StandrardProfile\\GloballyOpenPort\\List"
            logging.debug("Openning key" + firewarllruleskeypath) 
            firewallkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, firewarllruleskeypath , 0 , win32con.KEY_ALL_ACCESS )
            valuename = "3389:TCP"
            newvalue = "3389:TCP:*:Enabled:@xpsp2res.dll,-22009"
            win32api.RegSetValueEx(firewallkey, valuename , 0 , win32con.REG_SZ, newvalue)
            firewallkey.close()
        return 


    def adjustSystemHive(self ,hiveFilePath):
        
        logging.info("Adjusting the system hive") 

        originalwindir = os.environ['windir']
        windrive = originalwindir.split("\\")[0] #get C: substring

        # 1) mount hive from path
        hivekeyname = "MigrationSys"+str(int(time.mktime(time.localtime())))
        
        # A call to RegLoadKey fails if the calling process does not have the SE_RESTORE_PRIVILEGE privilege.
        token = win32security.OpenProcessToken(-1, win32con.TOKEN_ALL_ACCESS )
        privid = win32security.LookupPrivilegeValue(None, "SeRestorePrivilege" )
        privilages = win32security.AdjustTokenPrivileges(token, False , [(privid , win32security.SE_PRIVILEGE_ENABLED)])

        win32api.RegLoadKey(win32con.HKEY_LOCAL_MACHINE, hivekeyname, hiveFilePath)

        # 2) alter it: change keys (they should be in the config: some of keys should be added, some should be deleted)

        #To change: Mountdevs, driver entries , maybe setups, system\boot partitions
        #optionally: add my soft

        # TODO: WIN2003 there is no:
        # a) LSI_SCSI
        # b) Services\\SharedAccess\\Parameters\\FirewallPolicy\\FirewallRules
        # c) should locate firewalls \ rdp settings then


        # 2a) mountdevs
        mountdevkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\MountedDevices" , 0 , win32con.KEY_ALL_ACCESS )
        (oldvalue,valtype) = win32api.RegQueryValueEx(mountdevkey, "\\DosDevices\\"+windrive)
        newvalue =  struct.pack('=i',self.__adjustConfig.getNewMbrId()) + struct.pack('=q',self.__adjustConfig.getNewSysPartStart()) 
        win32api.RegSetValueEx(mountdevkey, "\\DosDevices\\"+windrive , 0, valtype, newvalue)
        #TODO: replace mountpoints for another vols\VolGuids too
        mountdevkey.close()

        logging.debug("Set the mountpoint for\\DosDevices\\"+windrive + " from " + str(oldvalue) + "to " + str(newvalue)) 

        # 2b) change firmware\system entires in \\CurrentControlSet\Control
        selectkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\Select" , 0 , win32con.KEY_READ )
        (currentcontrolset,valtype) = win32api.RegQueryValueEx(selectkey, "Current")
        selectkey.close()

        logging.debug("Current Control Set is " + str(currentcontrolset)) 

        
        #let's turn all of them by default
        diskSCSI = True
        diskIDE = True

        if self.__adjustConfig.getSysDiskType() == self.__adjustConfig.diskScsi:
            diskSCSI = True
            diskIDE = False
        if self.__adjustConfig.getSysDiskType() == self.__adjustConfig.diskAta:
            diskSCSI = False
            diskIDE = True
        
        controlsetkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Control" , 0 , win32con.KEY_ALL_ACCESS )
        win32api.RegSetValueEx(controlsetkey, "SystemBootDevice" , 0, win32con.REG_SZ, "multi(0)disk(0)rdisk(0)partition(1)")
        win32api.RegSetValueEx(controlsetkey, "FirmwareBootDevice" , 0, win32con.REG_SZ, "multi(0)disk(0)rdisk(0)partition(1)")
        controlsetkey.close()

        if diskSCSI:
            logging.info("Setting the SCSI driver as default") 
            # 2c) add lsiscsi to load on start
            lsikey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\LSI_SCSI" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(lsikey, "Start" , 0, win32con.REG_DWORD, 0)
            lsikey.close()
        
        if diskIDE:
            logging.info("Setting the IDE driver as default") 
            # 2c) add intelide\atapi to load on start
            idekey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\intelide" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(idekey, "Start" , 0, win32con.REG_DWORD, 0)
            idekey.close()

            atapikey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\atapi" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(atapikey, "Start" , 0, win32con.REG_DWORD, 0)
            atapikey.close()

        if self.__adjustConfig.fixRDP():
            turnRDP = True

        rdpport=3389 #default port
        if self.__adjustConfig.rdpPort():
            rdpport = self.__adjustConfig.rdpPort()
        
    
        if turnRDP:
            logging.info("Turning on RDP") 

            #NOTE: checked on 6.1 only! 
            #TODO: Should recheck on other versions!
            # 2d) add turn on firewall rdp feature
            self.enableRdpFirewall( hivekeyname , currentcontrolset)

            #2e) setting the rdp setting to ones needed
            #TODO: make kinda wrapper function!
            #TODO: check Windows 2003, how could the security be added
            terminalkeypath = hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Control\\Terminal Server\\WinStations\\RDP-TCP"
            logging.debug("Openning key" + terminalkeypath) 
            terminalkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, terminalkeypath , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(terminalkey, "UserAuthentication" , 0, win32con.REG_DWORD, 1)
            win32api.RegSetValueEx(terminalkey, "SecurityLayer" , 0, win32con.REG_DWORD, 1)
            win32api.RegSetValueEx(terminalkey, "fAllowSecProtocolNegotiation" , 0, win32con.REG_DWORD, 1)
            win32api.RegSetValueEx(terminalkey, "PortNumber" , 0 ,  win32con.REG_DWORD , rdpport)
            terminalkey.close()

            #2f) setting the allow flag
            terminalkeypath = hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Control\\Terminal Server"
            logging.debug("Openning key" + terminalkeypath) 
            terminalkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, terminalkeypath , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(terminalkey, "fDenyTSConnections" , 0, win32con.REG_DWORD, 0)
            terminalkey.close()

        #---------------- 2g) turniong on hyper-v bus so the machine would start in hyper-v
        turnHyperV = True
    
        if turnHyperV:
            logging.info("Turning on HyperV bus") 
            #Note: it is good for 6.0+ only. 
            hypervkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\vmbus" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(hypervkey, "Start" , 0, win32con.REG_DWORD, 0)
            hypervkey.close()

        #---------------- 2g) remove extra paltform-dependent virtualization services
        #TODO: make remove scripts
        removeCitrix = False
        removeHyperV = False
        removeVmware = False

        if removeCitrix:
            # removing all xen* services
            #xenkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\intelide" , 0 , win32con.KEY_ALL_ACCESS )
            #win32api.RegSetValueEx(xenkey, "Start" , 0, win32con.REG_DWORD, 0)
            #xenkey.close()
            # remove all xen* service keys + scsifilt
            # and then remove Software Citrix key. Should be enough
            # and remove this one too HKEY_LOCAL_MACHINE\sys\ControlSet001\Control\Class\{4D36E967-E325-11CE-BFC1-08002BE10318}\scsifilt (todo: check the right name once more)

            logging.info("Removing Citrix services") 

        if removeVmware:
            logging.info("Removing VmWare services") 
            vmwarekey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\vmtools" , 0 , win32con.KEY_ALL_ACCESS )
            win32api.RegSetValueEx(vmwarekey, "Start" , 0, win32con.REG_DWORD, 4)
            vmwarekey.close()
          
        if removeHyperV:
            logging.info("Removing HyperV services") 


        #---------------- 3) inject the service
        #Note: the service should be placed on windrive!
        installNotificationService = self.__adjustConfig.installNotifyService()
        if installNotificationService:
            servicepath = self.__adjustConfig.getNotificationServicePath();
            serviceconfigpath = self.__adjustConfig.getNotificationServiceConfigPath();
            servicekeyname = hivekeyname+"\\ControlSet00"+str(currentcontrolset)+"\\Services\\CloudscraperNotify"
            servicekey = win32api.RegCreateKey(win32con.HKEY_LOCAL_MACHINE, servicekeyname)
            win32api.RegSetValueEx(servicekey , "Start" , 0 , win32con.REG_DWORD , 2)
            win32api.RegSetValueEx(servicekey , "WOW64" , 0 , win32con.REG_DWORD , 1)
            win32api.RegSetValueEx(servicekey , "ErrorControl" , 0 , win32con.REG_DWORD , 1)
            win32api.RegSetValueEx(servicekey , "Type" , 0 , win32con.REG_DWORD , 0x10)
            win32api.RegSetValueEx(servicekey , "ObjectName" , 0 , win32con.REG_SZ , "LocalSystem")
            win32api.RegSetValueEx(servicekey , "DisplayName" , 0 , win32con.REG_SZ , "Cloudscraper Notification Service")
            win32api.RegSetValueEx(servicekey , "DependOnService" , 0 , win32con.REG_MULTI_SZ , "EventLog")
            win32api.RegSetValueEx(servicekey , "ImagePath" , 0 , win32con.REG_EXPAND_SZ , servicepath)
            win32api.RegSetValueEx(servicekey , "ConfigPath" , 0 , win32con.REG_EXPAND_SZ , serviceconfigpath)
            servicekey.close()

        #Do operations
        #for operation in self.__adjustConfig.getSystemHiveOperations():
        #    operation.Do(rootRegKey)

        # 4) dismount
        win32api.RegUnLoadKey(win32con.HKEY_LOCAL_MACHINE, hivekeyname)

        # 5) check the hive has the same size it was before

        # 6) maybe find a way to shrink it?
        return

    def adjustSoftwareHive(self ,hiveFilePath):
        
        logging.info("Adjusting the software hive") 

        originalwindir = os.environ['windir']
        windrive = originalwindir.split("\\")[0] #get C: substring

        # 1) mount hive from path
        hivekeyname = "MigrationSoft"+str(int(time.mktime(time.localtime())))
        
        # A call to RegLoadKey fails if the calling process does not have the SE_RESTORE_PRIVILEGE privilege.
        token = win32security.OpenProcessToken(-1, win32con.TOKEN_ALL_ACCESS )
        privid = win32security.LookupPrivilegeValue(None, "SeRestorePrivilege" )
        privilages = win32security.AdjustTokenPrivileges(token, False , [(privid , win32security.SE_PRIVILEGE_ENABLED)])

        win32api.RegLoadKey(win32con.HKEY_LOCAL_MACHINE, hivekeyname, hiveFilePath)

        # 2) alter it: delete keys if they exist

        #TODO: add to the adjust options
        removeCitrix = True
        removeHyperV = False
        removeVmware = False

        if removeCitrix:
            logging.info("Removing Citrix software keys") 
            # NOTE: works under Vista+ only
            try:
                # NOTE: seem to damage the hive, should test it more
                win32api.RegDeleteTree(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\Citrix")
            except Exception as e:
                logging.debug("Couldn't delete Citrix keys") 
                logging.debug(str(e))
      
      
                  
        removeAutologon = True
        if removeAutologon:
            logging.info("Removing Auto Logon") 
            winlogonkey = None
            try:
                # NOTE: seem to damage the hive, should test it more
                winlogonkey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, hivekeyname+"\\Microsoft\\Windows NT\\CurrentVersion\\Winlogon" , 0 , win32con.KEY_ALL_ACCESS )
                win32api.RegSetValueEx(winlogonkey, "AutoAdminLogon", 0 , win32con.REG_DWORD, 0)
                win32api.RegDeleteValue(winlogonkey, "DefaultPassword")
            except Exception as e:
                logging.debug("Couldn't remove Autologon") 
                logging.debug(str(e))
            finally:
                if winlogonkey:
                    winlogonkey.close()
      

        # 3) dismount
        win32api.RegUnLoadKey(win32con.HKEY_LOCAL_MACHINE, hivekeyname)

        # 4) check the hive has the same size it was before

        # 5) maybe find a way to shrink it?
        return

    # creates and adjusts new BCD registry hive returning path to it
    def generateBcd(self, backupSource):
        # we use pregenerated hive which we alter a bit in here
        originalhivepath = self.__adjustConfig.getPregeneratedBcdHivePath()
        hivepath = os.environ['TEMP']+"\\tempbcdhive"+str(int(time.mktime(time.localtime())))
        shutil.copy2(originalhivepath , hivepath)
        #TODO: copy hive to sorta temp dir
        bcdkeyname = "MigrationBCD"+str(int(time.mktime(time.localtime())))
        BCD_DETECTHAL_KEY = "26000010"
        BCD_DEVICE_KEY = "11000001"
        BCD_OS_DEVICE_KEY = "21000001"

        detecthal = self.__adjustConfig.detectHal()

        logging.info("Creating new BCD record") 

        # A call to RegLoadKey fails if the calling process does not have the SE_RESTORE_PRIVILEGE privilege.
        token = win32security.OpenProcessToken(-1, win32con.TOKEN_ALL_ACCESS )
        privid = win32security.LookupPrivilegeValue(None, "SeRestorePrivilege" )
        privilages = win32security.AdjustTokenPrivileges(token, False , [(privid , win32security.SE_PRIVILEGE_ENABLED)])

        win32api.RegLoadKey(win32con.HKEY_LOCAL_MACHINE, bcdkeyname, hivepath)
        logging.debug("Openning " + bcdkeyname+"\\Objects" ) 
        objectskey = win32api.RegOpenKeyEx(win32con.HKEY_LOCAL_MACHINE, bcdkeyname+"\\Objects" , 0 , win32con.KEY_ALL_ACCESS )
        guidskeys = win32api.RegEnumKeyEx(objectskey)
        for  (keyname, reserved, classname, modtime) in guidskeys:
            elementskey = win32api.RegOpenKeyEx(objectskey, keyname+"\\Elements" , 0 , win32con.KEY_ALL_ACCESS )
            valuekeys = win32api.RegEnumKeyEx(elementskey)
            for  (valkeyname, reserved, classname1, modtime1) in valuekeys:
                if valkeyname == BCD_DEVICE_KEY or valkeyname == BCD_OS_DEVICE_KEY:
                    logging.debug("Changing the element " + valkeyname + " of " + keyname + " bcd object" ) 

                    valuekey = win32api.RegOpenKeyEx(elementskey, valkeyname , 0 , win32con.KEY_ALL_ACCESS )
                    (value,valtype) = win32api.RegQueryValueEx(valuekey, "Element")
                    if valtype != win32con.REG_BINARY:
                        #TODO: err here
                        return
                    newvalue_part1 = value[0:0x20] + struct.pack('=q',self.__adjustConfig.getNewSysPartStart()) 
                    newvalue = newvalue_part1 + value[0x28:0x38] + struct.pack('=i',self.__adjustConfig.getNewMbrId()) + value[0x3c:] 
                    logging.debug("From " + value + "to " + newvalue) 
                    logging.debug("MBR ID = " + hex(self.__adjustConfig.getNewMbrId()) + " , PartOffset = " + hex(self.__adjustConfig.getNewSysPartStart()))
                    win32api.RegSetValueEx(valuekey, "Element", 0, valtype, newvalue)
                    valuekey.close()
                    # turn on detect hal option 
                    if detecthal:
                        valuekey = win32api.RegCreateKey(elementskey, BCD_DETECTHAL_KEY) 
                        logging.debug("Changing the element " + BCD_DETECTHAL_KEY + " of " + keyname + " bcd object: setting HAL autodetect" ) 
                        value = bytearray([0x01])
                        win32api.RegSetValueEx(valuekey, "Element", 0, win32con.REG_BINARY, value)
                        valuekey.close()
            elementskey.close()
        objectskey.close()

        win32api.RegUnLoadKey(win32con.HKEY_LOCAL_MACHINE , bcdkeyname)
        return hivepath
        # TODO: set original security access to the hive afterwards
                
      
    # creates and adjusts 
    def adjustBcd(self, backupSource):
        
        bootdir = "\\Boot"
        
        newbcd = self.generateBcd(backupSource)
        # check if bcd exists on backup source (vss snapshot)
        bcd_exists = True
        bcd_size = 0
        try:
            blocks = backupSource.getFileBlockRange(bootdir+"\\BCD")
            for block in blocks:
                bcd_size = bcd_size + block.getSize()
            logging.debug("Original BCD of size " + str(bcd_size) + " was found") 
        except:
            bcd_exists = False

        #TODO: note we may need some cleanup management after all theese copies
        logging.info("Adjusting BCD settings") 
        if ( bcd_exists ) :
            #found it on system drive, we should alter it somehow, easy
            logging.debug("BCD found, making replacement for it") 
            sizedelta = int(bcd_size - os.path.getsize(newbcd))
            if sizedelta > 0:
                logging.debug("New bcd is larger " + str(sizedelta) + " bytes than the new generated one") 
                logging.debug("Appending nulls") 
                #pad new bcd with 0 to fit the original size
                bcdfile = open(newbcd, "a+b")
                nulls = bytearray(sizedelta) # is filled with nulls by python
                bcdfile.write(nulls)
            if sizedelta < 0:
                logging.error("!!!ERROR: Cannot insert the new BCD because it's larger than the original one") 
                return 

            self.replaceFile("\\Boot\\BCD" , newbcd)
            return
        else:
            logging.error("!!!ERROR: BCD was not found on the original location!") 
            return
            
    def adjustHal(self, backupSource):

        # code for win2003, no hal fixes are needed in other versions
        # here we just extract the hal (halacpi.dll) to system32
        originalwindir = os.environ['windir']
        

       # halpath = originalwindir+"\\System32\\halacpi.dll"
       # if os.path.exists(halpath):
            #NOTE: we must install hal of correct SP of windows, older SPs are in this list too
            #self.replaceFile(windir+"\\system32\\hal.dll" , halpath)
        return
 

    def adjustRegistry(self , backupSource):

        originalwindir = os.environ['windir']
        windir = originalwindir.split(":\\")[-1] #get substring after X:\
        tmphiveroot = os.environ['TEMP']
        #TODO: more logs here
        sys_hive_path = tmphiveroot+"\\syshive"+str(int(time.mktime(time.localtime()))) 
        sysHiveTmp = open(sys_hive_path, "wb")
        extentsSysHive = backupSource.getFileBlockRange(windir+"\\system32\\config\\system")
        for extent in extentsSysHive:
            data_read = extent.getData()
            data_read_size = len(data_read)
            sysHiveTmp.write(data_read)

        sysHiveTmp.close() 

        self.adjustSystemHive(sys_hive_path)

        #NOTE: it works for local images only
        extentsSoftHive = backupSource.getFileBlockRange(windir+"\\system32\\config\\software")
        soft_hive_path = tmphiveroot+"\\softhive"+str(int(time.mktime(time.localtime()))) 
        softHiveTmp = open(soft_hive_path, "wb")
        for extent in extentsSoftHive:
            data_read = extent.getData()
            data_read_size = len(data_read)
            softHiveTmp.write(data_read)    

        softHiveTmp.close()
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

        
        
