setlocal
set OLD_CD= %CD%
cd /d "%~dp0"

set S3_OWNER=%2
set S3_KEY=%3
set EC2REGION=%4

call set_amazon_env.bat


%EC2_HOME%\bin\ec2-describe-conversion-tasks %1 -O %S3_OWNER% -W %S3_KEY%  --region %EC2REGION% 

cd %OLD_CD%
endlocal