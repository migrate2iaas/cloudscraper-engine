:: installs virtio networks\drivers etc

echo "Preved!" >> C:\adjustlog.txt

if defined ProgramFiles(x86) (
    @echo Some 64-bit work
  set DRVPATH=%~dp0\..\VirtIO\XP\AMD64
  set DEVCON=devcon_64
) else (
    @echo Some 32-bit work
  set DRVPATH=%~dp0\..\VirtIO\XP\X86
  set DEVCON=devcon_32
)

%~dp0\%DEVCON% install "%DRVPATH%\netkvm.inf" "PCI\VEN_1AF4&DEV_1000&SUBSYS_00011AF4&REV_00" >> C:\adjustlog.txt
