@echo off

set BASEDIR=%~dp0

net stop vds
net stop vss
net stop swprv
net stop "xensvc"

cscript "%BASEDIR%regvss.vbs" -unregister "XenServerVssProvider"
cscript "%BASEDIR%regvss.vbs" -unregister "XenVssProvider"
regsvr32 /s /u "%BASEDIR%XenVssProvider.dll"
cscript "%BASEDIR%regvss.vbs" -register "XenVssProvider" "%BASEDIR%XenVssProvider.dll" "Xen VSS Provider"

net start vds
net start vss
net start swprv
net start "xensvc"
