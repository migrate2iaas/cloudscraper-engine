
setlocal
set OLD_CD= %CD%
cd /d "%~dp0"

call set_amazon_env.bat
set TARGET_VM_PATH=%1
set TARGET_VMFORMAT=%2


rem no echo while pasword is set
set S3_OWNER=%3
@echo off
@set S3_KEY=%4
@echo on
set EC2_XML=%5
set EC2_ZONE=%6
set EC2REGION=%7


@call %EC2_HOME%\bin\ec2-import-volume.cmd %TARGET_VM_PATH%   -f %TARGET_VMFORMAT% -z %EC2_ZONE% -o %S3_OWNER%  -w %S3_KEY% --region %EC2REGION% -O %S3_OWNER% -W %S3_KEY% --manifest-url "%EC2_XML%"

cd %OLD_CD%
endlocal