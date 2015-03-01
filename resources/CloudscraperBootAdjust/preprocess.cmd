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

%difxcmd% /i "%~dp0\virtio\viostor.inf" 7
