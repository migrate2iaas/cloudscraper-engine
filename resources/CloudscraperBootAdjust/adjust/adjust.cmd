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

echo Wait till network device is up... >>  C:\adjustlog.txt
ping 128.0.0.1 -n 5 -w 600 >>  C:\adjustlog.txt
IPCONFIG >> C:\adjustlog.txt

if exist %~dp0\netsh_dump.txt (

cd "%~dp0"
echo Removing the absent devices >>  C:\adjustlog.txt
call removedevices.js /noconfirm /verbose >> C:\adjustlog.txt

:: Checks adapter name
SET adapterName=

FOR /F "tokens=* delims=:" %%a IN ('IPCONFIG ^| FIND /I "ETHERNET ADAPTER"') DO (
SET adapterName=%%a

REM Removes "Ethernet adapter" from the front of the adapter name
SET adapterName=!adapterName:~17!

REM Removes the colon from the end of the adapter name
SET adapterName=!adapterName:~0,-1!

echo Renaming !adapterName! to Local Area Connection >> C:\adjustlog.txt
:: renaming the primary ethernet to Local Area Network
netsh interface set interface name="!adapterName!" newname="Local Area Connection" >> C:\adjustlog.txt
)
:: executing the adjust script
echo "Importing tcpip settings" >> C:\adjustlog.txt
netsh -f "%~dp0\netsh_dump.txt" >> C:\adjustlog.txt
ping 128.0.0.1 -n 1 -w 6000 >>  C:\adjustlog.txt
del /Q "%~dp0\netsh_dump.txt" 
)
:EOF
