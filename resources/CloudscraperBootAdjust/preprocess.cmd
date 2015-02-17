rem if we run from 32-bit cmd on 64-bit windows
if exist C:\Windows\sysnative\pnputil.exe cd C:\Windows\sysnative

certutil -addstore TrustedPublisher "%~dp0"\adjust\redhat.cer
pnputil /a "%~dp0"\virtio\viostor.inf
