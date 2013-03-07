
setlocal
set OLD_CD= %CD%
cd /d "%~dp0"



call set_amazon_env.bat
set TARGET_VM_PATH=%1
set S3_OWNER=%2

rem no echo while pasword is set
@echo off
@set S3_KEY=%3
@echo on
set EC2_XML=%4
set EC2_ZONE=%5
set MACHINEARCH=%6
rem we cannot handle more than 9 parms in bat
set EC2REGION=%7
if "%MACHINETYPE%"=="" set MACHINETYPE=m1.small

@call %EC2_HOME%\bin\ec2-import-instance.cmd %TARGET_VM_PATH% -t %MACHINETYPE% -a %MACHINEARCH%  -f VHD -o %S3_OWNER%  -w %S3_KEY% -z %EC2_ZONE% --region %EC2REGION% -O %S3_OWNER% -W %S3_KEY% --manifest-url "%EC2_XML%"

cd %OLD_CD%
endlocal