rem if we run from 32-bit cmd on 64-bit windows
if exist C:\Windows\sysnative\pnputil.exe cd C:\Windows\sysnative

certutil -addstore TrustedPublisher "%~dp0"\adjust\redhat.cer

if defined ProgramFiles(x86) (
    @echo Some 64-bit work
  set difxcmd=difxcmd_64
) else (
    @echo Some 32-bit work
  set difxcmd=difxcmd_32
)

:: Get windows Version numbers
For /f "tokens=2 delims=[]" %%G in ('ver') Do (set _version=%%G) 

For /f "tokens=2,3,4 delims=. " %%G in ('echo %_version%') Do (set _major=%%G& set _minor=%%H& set _build=%%I) 

Echo Major version: %_major%  Minor Version: %_minor%.%_build%

:: win2003 or XP
if "%_major%"=="5" goto :EOF

pnputil /a "%~dp0\virtio\viostor.inf"