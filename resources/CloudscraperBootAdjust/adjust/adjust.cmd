SETLOCAL EnableDelayedExpansion

:: installs virtio networks\drivers etc

echo "Preved!" >> C:\adjustlog.txt

if defined ProgramFiles(x86) (
    @echo Some 64-bit work
  set DRVPATH=%~dp0\..\VirtIO\
  set DEVCON=devcon_64
) else (
    @echo Some 32-bit work
  set DRVPATH=%~dp0\..\VirtIO\
  set DEVCON=devcon_32
)
copy /Y "%~dp0\%DEVCON%.exe" "%~dp0\devcon.exe"

certutil -addstore TrustedPublisher redhat.cer >> C:\adjustlog.txt

"%~dp0\%DEVCON%" install "%DRVPATH%\netkvm.inf" "PCI\VEN_1AF4&DEV_1000&SUBSYS_00011AF4&REV_00" >> C:\adjustlog.txt

echo Wait several mins till network device is up... >>  C:\adjustlog.txt
CHOICE /C:AB /D:A /T 320 > NUL
IPCONFIG >> C:\adjustlog.txt
echo Quering WMI >> C:\adjustlog.txt
wmic NIC where NetEnabled=true get Name,NetConnectionID >> C:\adjustlog.txt
echo Quering netsh >> C:\adjustlog.txt
netsh interface show interface >> C:\adjustlog.txt

if exist %~dp0\netsh_dump.txt (

cd "%~dp0"
echo Removing the absent devices >>  C:\adjustlog.txt

:: Checks adapter name
SET cloudscraperNetName=VirtualNetworkAdapter

rem the network name should hardcoded somewhere in minipad Windows network configuration

wmic NIC where "NetEnabled=true and Name like 'Red Hat VirtIO%%'" get NetConnectionID /value >>  C:\adjustlog.txt

FOR /F "tokens=2 delims==" %%a IN ('wmic NIC where "NetEnabled=true and Name like 'Red Hat VirtIO%%'" get NetConnectionID /value ^| more ^| findstr /I "NetConnectionID" ') DO (
echo Renaming %%a to !cloudscraperNetName! >> C:\adjustlog.txt
:: renaming the primary ethernet to Local Area Network
netsh interface set interface name="%%a" newname="!cloudscraperNetName!" >> C:\adjustlog.txt
:: try the other interface name - the output may contain extra crlf characters
set interfaceName=%%a
set interfaceName=!interfaceName:~0,-1!
netsh interface set interface name="!interfaceName!" newname="!cloudscraperNetName!" >> C:\adjustlog.txt
)
:: executing the adjust script
echo "Importing tcpip settings" >> C:\adjustlog.txt
netsh -f "%~dp0\netsh_dump.txt" >> C:\adjustlog.txt
::send 10 packets with minute between each other
rem TODO: change this one to something pingable inside the cloud
ping 8.8.8.8 -n 10 -w 1000 | Findstr /I /C:"timed out" /C:"host unreachable" /C:"could not find host" /C:"error"  /C:"failed" >>  C:\adjustlog.txt
IF ERRORLEVEL 1 (
	echo Netconf applied. Deleting the network config file >>  C:\adjustlog.txt
	del /Q "%~dp0\netsh_dump.txt" >>  C:\adjustlog.txt
)

)

echo "Disable autoadjust" >>   C:\adjustlog.txt
sc config CloudscraperBoot start= demand >>   C:\adjustlog.txt

:EOF
